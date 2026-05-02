# frozen_string_literal: true

# Forwarder.dispatch handoff cost.
#
# The current implementation routes immediate rows through one
# ActiveJob.perform_all_later call, which collapses to a single
# enqueue_all on PG-backed adapters (good_job, solid_queue) or falls
# back to per-job enqueue on others (TestAdapter).
#
# We benchmark against TestAdapter (single-process, in-memory) so
# the numbers reflect the gem's overhead, not Postgres write cost.
# For a real adapter benchmark, point DUMMY_ADAPTER=good_job and
# write a separate harness that measures end-to-end Tick → adapter
# row visibility.

require_relative "bench_helper"

Bench.connect!
Bench.recreate_schema!

class BenchForwarderJob < ActiveJob::Base
  def perform(*); end
end

def bench_rows(n, scheduled: false)
  n.times.map do |i|
    {
      "policy_name"   => "bench_fwd",
      "partition_key" => "p#{i % 10}",
      "queue_name"    => nil,
      "job_class"     => "BenchForwarderJob",
      "job_data"      => {
        "job_id"     => "j#{i}",
        "job_class"  => "BenchForwarderJob",
        "arguments"  => [{}]
      },
      "context"       => {},
      "scheduled_at"  => scheduled ? Time.now + 60 : nil,
      "priority"      => 0,
      "enqueued_at"   => Time.now
    }
  end
end

Bench.section("Forwarder.dispatch (TestAdapter, immediate rows)") do |sec|
  [10, 100, 1_000].each do |n|
    median = Bench.measure_median_ms(runs: 3) do
      DispatchPolicy::Forwarder.dispatch(bench_rows(n))
    end
    rate = (n.to_f / (median / 1000.0)).round
    sec.row("#{n} jobs in one call", median, "jobs/sec": rate)
  end
end

Bench.section("Forwarder.dispatch with scheduled_at (per-row path)") do |sec|
  [10, 100].each do |n|
    median = Bench.measure_median_ms(runs: 3) do
      DispatchPolicy::Forwarder.dispatch(bench_rows(n, scheduled: true))
    end
    rate = (n.to_f / (median / 1000.0)).round
    sec.row("#{n} scheduled jobs", median, "jobs/sec": rate)
  end
end

Bench.section("Forwarder.dispatch mix (90% immediate, 10% scheduled)") do |sec|
  [100, 1_000].each do |n|
    immediate = bench_rows((n * 0.9).to_i, scheduled: false)
    scheduled = bench_rows((n * 0.1).to_i, scheduled: true)
    rows = (immediate + scheduled).shuffle
    median = Bench.measure_median_ms(runs: 3) do
      DispatchPolicy::Forwarder.dispatch(rows)
    end
    rate = (n.to_f / (median / 1000.0)).round
    sec.row("#{n} jobs (split path)", median, "jobs/sec": rate)
  end
end

Bench.print_report
