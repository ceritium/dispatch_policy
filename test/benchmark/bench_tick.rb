# frozen_string_literal: true

# Tick.run end-to-end at varying partition counts.
#
# What gets measured: wall clock from claim_partitions to tick_samples
# write, including the per-partition TX (DELETE staged + INSERT inflight
# + Forwarder.dispatch + decay update) and the bulk deny flush.
# The TestAdapter is used for Forwarder so we don't conflate the gem's
# admission cost with adapter INSERT cost.
#
# Scales: N partitions claimed per tick. partition_batch_size is fixed
# at 50 (the default), so even when seeding 10k partitions only 50 are
# touched per tick — what we want to measure is the admin overhead per
# tick AT that partition_batch_size, not the cost of N total partitions.

require_relative "bench_helper"

# Tick.run will deserialize staged rows and call enqueue on them.
# The job_class strings in the seed point to BenchJob — define it
# globally so deserialization succeeds.
class BenchJob < ActiveJob::Base
  def perform(*); end
end

Bench.connect!
Bench.recreate_schema!

# Load AR models so the Tick can record per-policy samples; without
# them the rescue swallows the error but emits a warning every tick.
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

Bench.section("Tick.run wall time (50 partitions admitted per tick, 5 staged each)") do |sec|
  [100, 1_000, 10_000].each do |total_partitions|
    Bench.register_policy!("bench_tick", batch_size: 5)

    median = Bench.measure_median_ms_with_setup(
      runs: Integer(ENV.fetch("RUNS", "5")),
      setup: -> {
        Bench.truncate!
        Bench.seed!("bench_tick", total_partitions, staged_per_partition: 5)
      },
      work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_tick") }
    )

    sec.row("#{total_partitions} partitions in DB", median,
            partitions_seen: 50, jobs_admitted_per_tick: 250)
  end
end

Bench.section("Tick.run with fairness reorder vs no reorder (1k partitions)") do |sec|
  # With reorder (default 60s half_life).
  Bench.register_policy!("bench_fair", batch_size: 5, half_life: 60)
  ms_with = Bench.measure_median_ms_with_setup(
    runs: 5,
    setup: -> {
      Bench.truncate!
      Bench.seed!("bench_fair", 1_000, staged_per_partition: 5)
    },
    work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_fair") }
  )
  sec.row("with reorder (half_life=60s)", ms_with)

  # Without reorder: half_life=nil disables both the SQL decay write
  # and the Ruby-side sort_by.
  DispatchPolicy.config.fairness_half_life_seconds = nil
  Bench.register_policy!("bench_fair", batch_size: 5)
  ms_without = Bench.measure_median_ms_with_setup(
    runs: 5,
    setup: -> {
      Bench.truncate!
      Bench.seed!("bench_fair", 1_000, staged_per_partition: 5)
    },
    work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_fair") }
  )
  sec.row("without reorder", ms_without)

  overhead_ms      = ms_with - ms_without
  overhead_percent = (overhead_ms / ms_without * 100).round(1)
  sec.row("→ overhead", overhead_ms, percent: "#{overhead_percent}%")

  DispatchPolicy.config.fairness_half_life_seconds = 60
end

Bench.section("Tick.run with global tick_admission_budget (50 partitions × 100 staged)") do |sec|
  # No cap: each partition admits up to admission_batch_size = 50.
  Bench.register_policy!("bench_cap", batch_size: 50)
  ms_no_cap = Bench.measure_median_ms_with_setup(
    runs: 3,
    setup: -> {
      Bench.truncate!
      Bench.seed!("bench_cap", 50, staged_per_partition: 100)
    },
    work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_cap") }
  )
  sec.row("no cap (admits up to 50 × 50 = 2500/tick)", ms_no_cap)

  # cap=200: fair_share = ceil(200/50) = 4 per partition.
  Bench.register_policy!("bench_cap", batch_size: 50, tick_cap: 200)
  ms_cap = Bench.measure_median_ms_with_setup(
    runs: 3,
    setup: -> {
      Bench.truncate!
      Bench.seed!("bench_cap", 50, staged_per_partition: 100)
    },
    work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_cap") }
  )
  sec.row("cap=200 (fair_share=4, with redistribution)", ms_cap)
end

Bench.print_report
