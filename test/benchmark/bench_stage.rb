# frozen_string_literal: true

# stage! and stage_many! throughput.
#
# stage! is the per-job hot path triggered by perform_later: one
# transaction per call, each doing INSERT staged + UPSERT partition.
# stage_many! is the path under perform_all_later (Rails 7.1+) — a
# single multi-row INSERT plus one UPSERT per (policy, partition_key).
#
# We measure jobs-per-second under each, plus the cost in ms per
# 1k jobs at varying partition fan-out.

require_relative "bench_helper"

Bench.connect!
Bench.recreate_schema!

def synth_row(key, idx)
  {
    policy_name:   "bench_stage",
    partition_key: key,
    queue_name:    nil,
    shard:         "default",
    job_class:     "BenchJob",
    job_data:      { "job_id" => "#{key}-#{idx}", "job_class" => "BenchJob", "arguments" => [{}] },
    context:       {},
    scheduled_at:  nil,
    priority:      0
  }
end

Bench.section("stage! (per-job, 1 TX per call)") do |sec|
  [100, 1_000].each do |n|
    Bench.truncate!
    median = Bench.measure_median_ms(runs: 3) do
      n.times do |i|
        DispatchPolicy::Repository.stage!(**synth_row("p#{i % 10}", i))
      end
    end
    rate = (n.to_f / (median / 1000.0)).round
    sec.row("#{n} jobs across 10 partitions", median, "jobs/sec": rate)
  end
end

Bench.section("stage_many! (one bulk INSERT)") do |sec|
  # PG bind-parameter limit is 65535. stage_many! uses 8 params per row,
  # so a single call tops out at ~8000 rows. The host should chunk on
  # top of stage_many! when going larger; the benchmark stays under
  # the limit.
  [100, 1_000, 5_000].each do |n|
    Bench.truncate!
    rows = n.times.map { |i| synth_row("p#{i % 10}", i) }
    median = Bench.measure_median_ms(runs: 3) do
      DispatchPolicy::Repository.stage_many!(rows)
    end
    rate = (n.to_f / (median / 1000.0)).round
    sec.row("#{n} jobs across 10 partitions", median, "jobs/sec": rate)
  end
end

Bench.section("stage_many! with high partition fan-out") do |sec|
  # n_partitions × 10 rows × 8 params <= 65535 → n_partitions <= 800.
  [10, 100, 500].each do |n_partitions|
    Bench.truncate!
    rows = (n_partitions * 10).times.map { |i| synth_row("p#{i % n_partitions}", i) }
    median = Bench.measure_median_ms(runs: 3) do
      DispatchPolicy::Repository.stage_many!(rows)
    end
    sec.row("#{n_partitions} partitions × 10 jobs each", median,
            "upserts": n_partitions, "rows": n_partitions * 10)
  end
end

Bench.print_report
