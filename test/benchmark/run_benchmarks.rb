# frozen_string_literal: true

# dispatch_policy benchmark runner — manual invocation only.
#
#   bundle exec rake test:benchmark
#   MAX_PARTITIONS=100000 bundle exec rake test:benchmark
#   BATCH_SIZE=1000 JOBS_PER_PARTITION=5 bundle exec rake test:benchmark
#
# stdout: column-aligned report (valid markdown, also readable in a
#         terminal). Capture cleanly with `… > report.md`.
# stderr: progress bars + status lines that are erased as each phase
#         finishes, so the final terminal view is just the report.
#
# Numbers depend on hardware and database (local PG vs containerised
# vs remote) — useful for spotting regressions and shaping defaults,
# NOT as an SLA.

require_relative "benchmark_helper"

include DispatchPolicy::BenchmarkHelpers

# ─── Job definitions ─────────────────────────────────────────────────

class TimeWeightedBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    context        ->(args) { { tenant: args.first } }
    round_robin_by ->(args) { args.first }, weight: :time
  end
  def perform(*); end
end

class RoundRobinBenchmarkJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    context        ->(args) { { tenant: args.first } }
    round_robin_by ->(args) { args.first }
  end
  def perform(*); end
end

class PlainBenchmarkJob < ActiveJob::Base
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

def seed_with_progress(label, **kwargs)
  total = kwargs[:partitions] * kwargs[:jobs_per_partition]
  bar = DispatchPolicy::BenchmarkHelpers::ProgressBar.new("#{label}: seeding", total)
  seeded = seed_staged!(**kwargs) { |so_far| bar.update(so_far) }
  bar.finish!
  seeded
end

# ─── Fetch / tick scenarios ──────────────────────────────────────────

def run_scenario(label, job_class:, scales:, jobs_per_partition:)
  policy = job_class.resolved_dispatch_policy
  rows_out = []

  scales.each do |n|
    truncate_all!
    seeded = seed_with_progress(
      "#{label} N=#{fmt_int(n)}",
      job_class:          job_class,
      partitions:         n,
      jobs_per_partition: jobs_per_partition
    )

    status("#{label} N=#{fmt_int(n)}: measuring…")
    wall_ms, sql_count, result = measure { yield(policy) }
    clear_status

    fetched = if result.is_a?(Integer)
      result
    elsif result.respond_to?(:size)
      result.size
    else
      0
    end

    rows_per_s = wall_ms.zero? ? 0 : (fetched * 1000.0 / wall_ms).round

    rows_out << [
      fmt_int(n),
      fmt_int(seeded),
      fmt_ms(wall_ms),
      fmt_int(sql_count),
      fmt_int(fetched),
      fmt_int(rows_per_s)
    ]
  end

  print_section(label)
  print_table(
    [ "partitions", "pending", "wall_ms", "sql", "rows", "rows/s" ],
    rows_out
  )
end

# 1. Time-weighted batch fetch.
run_scenario(
  "fetch_time_weighted_batch",
  job_class:          TimeWeightedBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.fetch_time_weighted_batch(policy)
end

# 2. Plain round-robin batch fetch.
run_scenario(
  "fetch_round_robin_batch",
  job_class:          RoundRobinBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.fetch_round_robin_batch(policy)
end

# 3. End-to-end Tick.run.
run_scenario(
  "Tick.run end-to-end (concurrency gate)",
  job_class:          PlainBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.run(policy_name: policy.name)
end

# 4. Tick.reap with N expired leases.
reap_rows = []
policy = PlainBenchmarkJob.resolved_dispatch_policy
SCALES.each do |n|
  truncate_all!
  seed_with_progress(
    "reap N=#{fmt_int(n)}",
    job_class:          PlainBenchmarkJob,
    partitions:         n,
    jobs_per_partition: 1
  )

  status("reap N=#{fmt_int(n)}: marking expired…")
  past = 1.hour.ago
  DispatchPolicy::StagedJob.where(policy_name: policy.name).update_all(
    admitted_at:      past,
    lease_expires_at: past,
    active_job_id:    "bench",
    partitions:       { concurrency: "p0" }
  )

  expired = DispatchPolicy::StagedJob.expired_leases.count
  status("reap N=#{fmt_int(n)}: reaping…")
  wall_ms, sql_count, _ = measure { DispatchPolicy::Tick.reap }
  clear_status

  rows_per_s = wall_ms.zero? ? 0 : (expired * 1000.0 / wall_ms).round
  reap_rows << [ fmt_int(expired), fmt_ms(wall_ms), fmt_int(sql_count), fmt_int(rows_per_s) ]
end
print_section("Tick.reap (all rows expired)")
print_table([ "rows", "wall_ms", "sql", "rows/s" ], reap_rows)

# 5. StagedJob.stage_many! bulk insert (seeding-cost baseline).
stage_rows = []
policy = TimeWeightedBenchmarkJob.resolved_dispatch_policy
SCALES.each do |n|
  truncate_all!
  status("stage_many! N=#{fmt_int(n)}: building job instances…")
  jobs = Array.new(n) { |i| TimeWeightedBenchmarkJob.new("p#{i}") }
  status("stage_many! N=#{fmt_int(n)}: inserting…")
  wall_ms, sql_count, inserted = measure do
    DispatchPolicy::StagedJob.stage_many!(policy: policy, jobs: jobs)
  end
  clear_status

  rows_per_s = wall_ms.zero? ? 0 : (inserted * 1000.0 / wall_ms).round
  stage_rows << [ fmt_int(inserted), fmt_ms(wall_ms), fmt_int(sql_count), fmt_int(rows_per_s) ]
end
print_section("StagedJob.stage_many! (bulk insert)")
print_table([ "jobs", "wall_ms", "sql", "rows/s" ], stage_rows)

truncate_all!
puts ""
puts "Done."
puts ""
