# frozen_string_literal: true

# dispatch_policy benchmark runner — manual invocation only.
#
#   bundle exec rake test:benchmark
#   MAX_PARTITIONS=100000 bundle exec rake test:benchmark
#   BATCH_SIZE=1000 JOBS_PER_PARTITION=5 bundle exec rake test:benchmark
#
# Prints a markdown table of wall-time and SQL-count measurements at
# increasing partition counts. Numbers are dependent on hardware and
# database (local Postgres vs containerised vs remote) — useful for
# spotting regressions and shaping defaults, NOT as an SLA.

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

# Default ladder. 100k is opt-in via MAX_PARTITIONS=100000 because
# seeding 300k rows takes meaningful wall time on its own.
SCALES = [ 100, 1_000, 10_000, 100_000 ].select { |n| n <= MAX_PARTITIONS }.freeze

raise "MAX_PARTITIONS must allow at least 100" if SCALES.empty?

puts ""
puts "DispatchPolicy benchmark"
puts "  batch_size:         #{BATCH_SIZE}"
puts "  jobs_per_partition: #{JOBS_PER_PARTITION}"
puts "  scales:             #{SCALES.inspect}"
puts "  pg version:         #{ActiveRecord::Base.connection.execute('SHOW server_version').first['server_version']}"
puts ""

# ─── Scenarios ───────────────────────────────────────────────────────
#
# Each scenario seeds N partitions × JOBS_PER_PARTITION rows, then
# measures the operation in isolation. Truncate between scenarios so
# rows from previous runs don't bleed into the measurement.

def run_scenario(label, job_class:, scales:, jobs_per_partition:)
  policy = job_class.resolved_dispatch_policy

  puts "## #{label}"
  puts ""
  puts "| partitions | pending | wall_ms | sql | rows | rows/s |"
  puts "|-----------:|--------:|--------:|----:|-----:|-------:|"

  scales.each do |n|
    DispatchPolicy::BenchmarkHelpers.truncate_all!
    seeded = DispatchPolicy::BenchmarkHelpers.seed_staged!(
      job_class:          job_class,
      partitions:         n,
      jobs_per_partition: jobs_per_partition
    )

    wall_ms, sql_count, result = DispatchPolicy::BenchmarkHelpers.measure { yield(policy) }

    rows = if result.is_a?(Integer)
      result
    elsif result.respond_to?(:size)
      result.size
    else
      0
    end

    rows_per_s = wall_ms.zero? ? 0 : (rows * 1000.0 / wall_ms).round
    puts "| #{fmt_int(n)} | #{fmt_int(seeded)} | #{fmt_ms(wall_ms)} | #{sql_count} | #{rows} | #{fmt_int(rows_per_s)} |"
  end
  puts ""
end

# 1. Time-weighted batch fetch (the operation we just refactored).
run_scenario(
  "fetch_time_weighted_batch",
  job_class:          TimeWeightedBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.fetch_time_weighted_batch(policy)
end

# 2. Plain round-robin batch fetch (already a single CTE+LATERAL).
run_scenario(
  "fetch_round_robin_batch",
  job_class:          RoundRobinBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.fetch_round_robin_batch(policy)
end

# 3. End-to-end Tick.run (gates + admit + adapter enqueue, all in TX).
run_scenario(
  "Tick.run end-to-end (concurrency gate, no fairness)",
  job_class:          PlainBenchmarkJob,
  scales:             SCALES,
  jobs_per_partition: JOBS_PER_PARTITION
) do |policy|
  DispatchPolicy::Tick.run(policy_name: policy.name)
end

# 4. Tick.reap with N expired leases. Stresses the per-row update +
#    counter decrement path. Seed admitted rows with already-expired
#    leases to simulate a backlog of stuck workers.
puts "## Tick.reap (all rows expired)"
puts ""
puts "| rows | wall_ms | sql | rows/s |"
puts "|-----:|--------:|----:|-------:|"

policy = PlainBenchmarkJob.resolved_dispatch_policy
SCALES.each do |n|
  DispatchPolicy::BenchmarkHelpers.truncate_all!
  DispatchPolicy::BenchmarkHelpers.seed_staged!(
    job_class:          PlainBenchmarkJob,
    partitions:         n,
    jobs_per_partition: 1
  )

  # Mark all as admitted with expired leases. Use update_all so we
  # don't pay per-row callbacks during seed.
  past = 1.hour.ago
  DispatchPolicy::StagedJob.where(policy_name: policy.name).update_all(
    admitted_at:      past,
    lease_expires_at: past,
    active_job_id:    "bench",
    partitions:       { concurrency: "p0" }
  )

  expired = DispatchPolicy::StagedJob.expired_leases.count
  wall_ms, sql_count, _ = DispatchPolicy::BenchmarkHelpers.measure { DispatchPolicy::Tick.reap }
  rows_per_s = wall_ms.zero? ? 0 : (expired * 1000.0 / wall_ms).round
  puts "| #{fmt_int(expired)} | #{fmt_ms(wall_ms)} | #{sql_count} | #{fmt_int(rows_per_s)} |"
end
puts ""

# 5. StagedJob.stage_many! bulk insert path. Useful as a baseline:
#    seeding cost lower bound for the rest of the benchmarks.
puts "## StagedJob.stage_many! (bulk insert)"
puts ""
puts "| jobs | wall_ms | sql | rows/s |"
puts "|-----:|--------:|----:|-------:|"

policy = TimeWeightedBenchmarkJob.resolved_dispatch_policy
SCALES.each do |n|
  DispatchPolicy::BenchmarkHelpers.truncate_all!
  jobs = Array.new(n) { |i| TimeWeightedBenchmarkJob.new("p#{i}") }
  wall_ms, sql_count, inserted = DispatchPolicy::BenchmarkHelpers.measure do
    DispatchPolicy::StagedJob.stage_many!(policy: policy, jobs: jobs)
  end
  rows_per_s = wall_ms.zero? ? 0 : (inserted * 1000.0 / wall_ms).round
  puts "| #{fmt_int(inserted)} | #{fmt_ms(wall_ms)} | #{sql_count} | #{fmt_int(rows_per_s)} |"
end
puts ""

DispatchPolicy::BenchmarkHelpers.truncate_all!
puts "Done."
