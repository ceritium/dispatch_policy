# frozen_string_literal: true

# dispatch_policy benchmark runner — manual invocation only.
#
#   bundle exec rake test:benchmark
#   MAX_PARTITIONS=100000 bundle exec rake test:benchmark
#   BATCH_SIZE=1000 JOBS_PER_PARTITION=5 bundle exec rake test:benchmark
#
# stdout: column-aligned report (valid markdown, also readable in a
#         terminal). Capture cleanly with `… > report.md`.
# stderr: progress bars + status lines that erase as each phase
#         finishes, so the final terminal view is just the report.
#
# Iteration is by scale, not by scenario, so read-only fetches can
# share a seed: only 4 seeds happen per scale (a fetch seed, a
# Tick.run seed, a reap seed, and the stage_many measurement which
# is its own seed) instead of 5.
#
# Numbers depend on hardware and database (local PG vs containerised
# vs remote) — useful for spotting regressions and shaping defaults,
# NOT as an SLA.

require_relative "benchmark_helper"

include DispatchPolicy::BenchmarkHelpers

# ─── Job definitions ─────────────────────────────────────────────────

# Used as the seed for BOTH fetch tests. fetch_round_robin_batch
# doesn't read the time-weight, so calling it against a time-weighted
# policy is fine — only the row shape matters and it's identical.
class FetchBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    context        ->(args) { { tenant: args.first } }
    round_robin_by ->(args) { args.first }, weight: :time
  end
  def perform(*); end
end

# Used for Tick.run + Tick.reap measurements. A concurrency gate
# means Tick.run does counter increments per admission, which is the
# meaningful per-job cost we want to measure.
class GatedBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    context ->(args) { { tenant: args.first } }
    gate :concurrency, max: 1_000_000, partition_by: ->(ctx) { ctx[:tenant] }
  end
  def perform(*); end
end

# ─── Configuration ───────────────────────────────────────────────────

BATCH_SIZE          = (ENV["BATCH_SIZE"]          || 500).to_i
JOBS_PER_PARTITION  = (ENV["JOBS_PER_PARTITION"]  || 3).to_i
MAX_PARTITIONS      = (ENV["MAX_PARTITIONS"]      || 10_000).to_i

DispatchPolicy.config.batch_size = BATCH_SIZE

SCALES = [ 100, 1_000, 10_000, 100_000 ].select { |n| n <= MAX_PARTITIONS }.freeze
raise "MAX_PARTITIONS must allow at least 100" if SCALES.empty?

pg_version = ActiveRecord::Base.connection.execute("SHOW server_version").first["server_version"]

puts ""
puts "═" * 72
puts " DispatchPolicy benchmark"
puts "═" * 72
puts "  batch_size:         #{BATCH_SIZE}"
puts "  jobs_per_partition: #{JOBS_PER_PARTITION}"
puts "  scales:             #{SCALES.map { |n| fmt_int(n) }.join(', ')}"
puts "  postgres:           #{pg_version}"

# ─── Helpers ─────────────────────────────────────────────────────────

def fetch_row(n, seeded, wall_ms, sql_count, fetched)
  rows_per_s = wall_ms.zero? ? 0 : (fetched * 1000.0 / wall_ms).round
  [
    fmt_int(n),
    fmt_int(seeded),
    fmt_ms(wall_ms),
    fmt_int(sql_count),
    fmt_int(fetched),
    fmt_int(rows_per_s)
  ]
end

def reap_row(n, wall_ms, sql_count)
  rows_per_s = wall_ms.zero? ? 0 : (n * 1000.0 / wall_ms).round
  [ fmt_int(n), fmt_ms(wall_ms), fmt_int(sql_count), fmt_int(rows_per_s) ]
end

def stage_row(n, wall_ms, sql_count)
  rows_per_s = wall_ms.zero? ? 0 : (n * 1000.0 / wall_ms).round
  [ fmt_int(n), fmt_ms(wall_ms), fmt_int(sql_count), fmt_int(rows_per_s) ]
end

# ─── Measurements ────────────────────────────────────────────────────

results = {
  fetch_time_weighted: [],
  fetch_round_robin:   [],
  tick_run:            [],
  reap:                [],
  stage_many:          []
}

fetch_policy = FetchBenchmarkJob.resolved_dispatch_policy
gated_policy = GatedBenchmarkJob.resolved_dispatch_policy

# Set to "0" to skip ANALYZE between seed and measurement. Default ON
# because insert_all-based seeding bypasses autovacuum and leaves stats
# stale, which flips PG into a worse plan and inflates fetch wall-time
# by 10-50x — not representative of steady-state production behaviour.
ANALYZE_AFTER_SEED = ENV["ANALYZE_AFTER_SEED"] != "0"

def analyze_staged!(label, n)
  return unless ANALYZE_AFTER_SEED
  status("#{label} N=#{fmt_int(n)}: ANALYZE staged_jobs…")
  ActiveRecord::Base.connection.execute("ANALYZE dispatch_policy_staged_jobs")
end

SCALES.each do |n|
  # ── Phase A: shared seed for both fetch_* measurements ───────────
  truncate_all!
  seeded = seed_with_progress(
    "fetch N=#{fmt_int(n)}",
    job_class:          FetchBenchmarkJob,
    partitions:         n,
    jobs_per_partition: JOBS_PER_PARTITION
  )
  analyze_staged!("fetch", n)

  status("fetch_time_weighted N=#{fmt_int(n)}: measuring…")
  wall_ms, sql_count, result = measure { DispatchPolicy::Tick.fetch_time_weighted_batch(fetch_policy) }
  fetched = result.respond_to?(:size) ? result.size : 0
  results[:fetch_time_weighted] << fetch_row(n, seeded, wall_ms, sql_count, fetched)
  clear_status

  status("fetch_round_robin N=#{fmt_int(n)}: measuring…")
  wall_ms, sql_count, result = measure { DispatchPolicy::Tick.fetch_round_robin_batch(fetch_policy) }
  fetched = result.respond_to?(:size) ? result.size : 0
  results[:fetch_round_robin] << fetch_row(n, seeded, wall_ms, sql_count, fetched)
  clear_status

  # ── Phase B: Tick.run consumes its seed (marks rows admitted) ────
  truncate_all!
  seeded = seed_with_progress(
    "Tick.run N=#{fmt_int(n)}",
    job_class:          GatedBenchmarkJob,
    partitions:         n,
    jobs_per_partition: JOBS_PER_PARTITION
  )
  analyze_staged!("Tick.run", n)

  status("Tick.run N=#{fmt_int(n)}: measuring…")
  wall_ms, sql_count, admitted = measure { DispatchPolicy::Tick.run(policy_name: gated_policy.name) }
  results[:tick_run] << fetch_row(n, seeded, wall_ms, sql_count, admitted)
  clear_status

  # ── Phase C: Tick.reap needs N admitted-and-expired rows ─────────
  truncate_all!
  seed_with_progress(
    "reap N=#{fmt_int(n)}",
    job_class:          GatedBenchmarkJob,
    partitions:         n,
    jobs_per_partition: 1
  )
  analyze_staged!("reap", n)

  status("reap N=#{fmt_int(n)}: marking expired…")
  past = 1.hour.ago
  DispatchPolicy::StagedJob.where(policy_name: gated_policy.name).update_all(
    admitted_at:      past,
    lease_expires_at: past,
    active_job_id:    "bench",
    partitions:       { concurrency: "p0" }
  )

  expired = DispatchPolicy::StagedJob.expired_leases.count
  status("reap N=#{fmt_int(n)}: reaping…")
  wall_ms, sql_count, _ = measure { DispatchPolicy::Tick.reap }
  results[:reap] << reap_row(expired, wall_ms, sql_count)
  clear_status

  # ── Phase D: stage_many (the measurement IS the seed) ────────────
  truncate_all!
  status("stage_many N=#{fmt_int(n)}: building job instances…")
  jobs = Array.new(n) { |i| GatedBenchmarkJob.new("p#{i}") }
  status("stage_many N=#{fmt_int(n)}: inserting…")
  wall_ms, sql_count, inserted = measure do
    DispatchPolicy::StagedJob.stage_many!(policy: gated_policy, jobs: jobs)
  end
  results[:stage_many] << stage_row(inserted, wall_ms, sql_count)
  clear_status
end

truncate_all!

# ─── Output ──────────────────────────────────────────────────────────

print_section("fetch_time_weighted_batch")
print_table([ "partitions", "pending", "wall_ms", "sql", "rows", "rows/s" ], results[:fetch_time_weighted])

print_section("fetch_round_robin_batch")
print_table([ "partitions", "pending", "wall_ms", "sql", "rows", "rows/s" ], results[:fetch_round_robin])

print_section("Tick.run end-to-end (concurrency gate)")
print_table([ "partitions", "pending", "wall_ms", "sql", "rows", "rows/s" ], results[:tick_run])

print_section("Tick.reap (all rows expired)")
print_table([ "rows", "wall_ms", "sql", "rows/s" ], results[:reap])

print_section("StagedJob.stage_many! (bulk insert)")
print_table([ "jobs", "wall_ms", "sql", "rows/s" ], results[:stage_many])

puts ""
puts "Done."
puts ""
