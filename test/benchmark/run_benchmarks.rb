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
# Numbers depend on hardware and database (local PG vs containerised
# vs remote) — useful for spotting regressions and shaping defaults,
# NOT as an SLA.

require_relative "benchmark_helper"

include DispatchPolicy::BenchmarkHelpers

# ─── Job definitions ─────────────────────────────────────────────────

# Concurrency-only — shape A. Most representative of the common case
# where caps are absolute integers per partition.
class ConcurrencyBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    partition_by ->(args) { args.first }
    concurrency  max: 1_000_000   # huge so cap never blocks during the bench
  end
  def perform(*); end
end

# Throttle-only — shape B. Exercises the token bucket math and the
# refill clamp on the dispatch path.
class ThrottleBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    partition_by ->(args) { args.first }
    throttle     rate: 1_000_000, per: 1.second, burst: 1_000_000
  end
  def perform(*); end
end

# Concurrency + throttle — shape C. Both predicates active in
# pluck_admissible's LEAST(...).
class CombinedBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    partition_by ->(args) { args.first }
    concurrency  max: 1_000_000
    throttle     rate: 1_000_000, per: 1.second, burst: 1_000_000
  end
  def perform(*); end
end

# ─── Configuration ───────────────────────────────────────────────────

BATCH_SIZE         = (ENV["BATCH_SIZE"]         || 500).to_i
JOBS_PER_PARTITION = (ENV["JOBS_PER_PARTITION"] || 3).to_i
MAX_PARTITIONS     = (ENV["MAX_PARTITIONS"]     || 10_000).to_i

DispatchPolicy.config.batch_size = BATCH_SIZE

SCALES = [ 100, 1_000, 10_000, 100_000 ].select { |n| n <= MAX_PARTITIONS }.freeze
raise "MAX_PARTITIONS must allow at least 100" if SCALES.empty?

pg_version = ActiveRecord::Base.connection.execute("SHOW server_version").first["server_version"]

puts ""
puts "═" * 72
puts " DispatchPolicy benchmark — concurrency + throttle"
puts "═" * 72
puts "  batch_size:         #{BATCH_SIZE}"
puts "  jobs_per_partition: #{JOBS_PER_PARTITION}"
puts "  scales:             #{SCALES.map { |n| fmt_int(n) }.join(', ')}"
puts "  postgres:           #{pg_version}"

# ─── Helpers ─────────────────────────────────────────────────────────

def row(n, seeded, wall_ms, sql_count, admitted)
  rows_per_s = wall_ms.zero? ? 0 : (admitted * 1000.0 / wall_ms).round
  [
    fmt_int(n),
    fmt_int(seeded),
    fmt_ms(wall_ms),
    fmt_int(sql_count),
    fmt_int(admitted),
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
  dispatch_concurrency: [],
  dispatch_throttle:    [],
  dispatch_combined:    [],
  reap:                 [],
  stage_many:           []
}

ANALYZE_AFTER_SEED = ENV["ANALYZE_AFTER_SEED"] != "0"

def analyze!(label, n)
  return unless ANALYZE_AFTER_SEED
  status("#{label} N=#{fmt_int(n)}: ANALYZE…")
  ActiveRecord::Base.connection.execute("ANALYZE dispatch_policy_staged_jobs")
  ActiveRecord::Base.connection.execute("ANALYZE dispatch_policy_partitions")
end

run_dispatch_benchmark = ->(label, key, job_class) {
  policy = job_class.resolved_dispatch_policy
  SCALES.each do |n|
    truncate_all!
    seeded = seed_with_progress(
      "#{label} N=#{fmt_int(n)}",
      job_class:          job_class,
      partitions:         n,
      jobs_per_partition: JOBS_PER_PARTITION
    )
    analyze!(label, n)

    status("#{label} N=#{fmt_int(n)}: dispatching…")
    wall_ms, sql_count, admitted = measure { DispatchPolicy::Dispatch.run(policy_name: policy.name) }
    results[key] << row(n, seeded, wall_ms, sql_count, admitted)
    clear_status
  end
}

run_dispatch_benchmark.call("concurrency", :dispatch_concurrency, ConcurrencyBenchmarkJob)
run_dispatch_benchmark.call("throttle",    :dispatch_throttle,    ThrottleBenchmarkJob)
run_dispatch_benchmark.call("combined",    :dispatch_combined,    CombinedBenchmarkJob)

# ─── Reap (all rows expired) ─────────────────────────────────────────

policy = ConcurrencyBenchmarkJob.resolved_dispatch_policy
SCALES.each do |n|
  truncate_all!
  seed_with_progress(
    "reap N=#{fmt_int(n)}",
    job_class:          ConcurrencyBenchmarkJob,
    partitions:         n,
    jobs_per_partition: 1
  )
  analyze!("reap", n)

  status("reap N=#{fmt_int(n)}: marking expired…")
  past = 1.hour.ago
  DispatchPolicy::StagedJob.where(policy_name: policy.name).update_all(
    admitted_at:      past,
    lease_expires_at: past,
    active_job_id:    "bench"
  )
  # Bump in_flight on partitions so reap has something to decrement.
  DispatchPolicy::PolicyPartition.where(policy_name: policy.name)
    .update_all("in_flight = pending_count, pending_count = 0")

  expired = DispatchPolicy::StagedJob.expired_leases.count
  status("reap N=#{fmt_int(n)}: reaping…")
  wall_ms, sql_count, _ = measure { DispatchPolicy::Dispatch.reap }
  results[:reap] << reap_row(expired, wall_ms, sql_count)
  clear_status
end

# ─── stage_many ──────────────────────────────────────────────────────

SCALES.each do |n|
  truncate_all!
  status("stage_many N=#{fmt_int(n)}: building job instances…")
  jobs = Array.new(n) { |i| ConcurrencyBenchmarkJob.new("p#{i}") }
  status("stage_many N=#{fmt_int(n)}: inserting…")
  wall_ms, sql_count, inserted = measure do
    DispatchPolicy::StagedJob.stage_many!(policy: policy, jobs: jobs)
  end
  results[:stage_many] << stage_row(inserted, wall_ms, sql_count)
  clear_status
end

truncate_all!

# ─── Output ──────────────────────────────────────────────────────────

print_section("Dispatch.run — concurrency only")
print_table([ "partitions", "pending", "wall_ms", "sql", "admitted", "rows/s" ], results[:dispatch_concurrency])

print_section("Dispatch.run — throttle only")
print_table([ "partitions", "pending", "wall_ms", "sql", "admitted", "rows/s" ], results[:dispatch_throttle])

print_section("Dispatch.run — concurrency + throttle")
print_table([ "partitions", "pending", "wall_ms", "sql", "admitted", "rows/s" ], results[:dispatch_combined])

print_section("Dispatch.reap (all rows expired)")
print_table([ "rows", "wall_ms", "sql", "rows/s" ], results[:reap])

print_section("StagedJob.stage_many! (bulk insert)")
print_table([ "jobs", "wall_ms", "sql", "rows/s" ], results[:stage_many])

puts ""
puts "Done."
puts ""
