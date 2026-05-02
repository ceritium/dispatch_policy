# frozen_string_literal: true

# End-to-end Tick.run against a real PG-backed ActiveJob adapter.
#
# The other benchmarks use the in-memory TestAdapter so their numbers
# reflect the gem's overhead in isolation. This one wires the real
# adapter (good_job by default; solid_queue if BENCH_ADAPTER=solid_queue
# and the DB has its tables) and includes the actual INSERT into
# good_jobs / solid_queue_jobs in the wall time. The delta versus the
# TestAdapter run is the "cost of the adapter handoff" specifically.
#
# Setup:
#
#   1. Run `bin/dummy setup good_job` once so the `dispatch_policy_dummy`
#      DB has both the gem's tables AND the adapter's tables.
#   2. Stop the dummy foreman (this bench TRUNCATEs the same tables).
#   3. BENCH_DB_NAME=dispatch_policy_dummy bundle exec ruby \
#         test/benchmark/bench_real_adapter.rb
#
#   For solid_queue:
#      BENCH_DB_NAME=dispatch_policy_dummy_solid_queue \
#      BENCH_ADAPTER=solid_queue \
#      bundle exec ruby test/benchmark/bench_real_adapter.rb
#
# Numbers reported:
#   - "test (gem only)"  — Forwarder.dispatch hits TestAdapter, no
#     INSERT to a Postgres adapter table. Same as bench_tick's
#     comparable scenario.
#   - "<adapter>"        — Forwarder.dispatch goes through the real
#     adapter; each admitted job becomes a row in good_jobs (or
#     solid_queue_jobs).
#   - "Δ adapter cost"   — wall-time difference, the slice that
#     dominates if you turn the gem off.

# Defaults BEFORE the helper / dummy app load.
ENV["DB_NAME"]       ||= ENV["BENCH_DB_NAME"] || "dispatch_policy_dummy"
ENV["DUMMY_ADAPTER"] ||= ENV["BENCH_ADAPTER"] || "good_job"
ENV["RAILS_ENV"]     ||= "development"
ENV["DATABASE_URL"]  ||= "postgres://#{ENV.fetch('DB_USER', ENV['USER'])}@" \
                        "#{ENV.fetch('DB_HOST', 'localhost')}/#{ENV['DB_NAME']}"

ADAPTER_NAME = ENV["DUMMY_ADAPTER"].to_sym

# Boot the dummy Rails app. good_job and solid_queue both require a
# Rails.application to be initialized — Rails.application.initialized?
# is checked at queue_adapter assignment.
require File.expand_path("../dummy/config/environment", __dir__)

require_relative "bench_helper"

ADAPTER_TABLE = case ADAPTER_NAME
                when :good_job    then "good_jobs"
                when :solid_queue then "solid_queue_jobs"
                else
                  abort "BENCH_ADAPTER must be 'good_job' or 'solid_queue', got #{ADAPTER_NAME.inspect}"
                end

Bench.connect!

unless ActiveRecord::Base.connection.table_exists?(ADAPTER_TABLE)
  abort <<~ERR
    Table #{ADAPTER_TABLE} not found in DB #{ENV.fetch('DB_NAME')}.

    Adapter '#{ADAPTER_NAME}' isn't set up there. Easiest fix:
      bin/dummy setup #{ADAPTER_NAME}
    then re-run this benchmark with:
      BENCH_DB_NAME=dispatch_policy_dummy BENCH_ADAPTER=#{ADAPTER_NAME} \\
        bundle exec ruby test/benchmark/bench_real_adapter.rb
  ERR
end

# Make sure the gem's fairness columns exist (the dummy DB was created
# before the fairness branch added them).
gem_cols = ActiveRecord::Base.connection.columns("dispatch_policy_partitions").map(&:name)
unless gem_cols.include?("decayed_admits")
  warn "[bench] Adding fairness columns to #{ENV['DB_NAME']}..."
  ActiveRecord::Base.connection.execute(<<~SQL)
    ALTER TABLE dispatch_policy_partitions
      ADD COLUMN IF NOT EXISTS decayed_admits double precision NOT NULL DEFAULT 0.0,
      ADD COLUMN IF NOT EXISTS decayed_admits_at timestamp
  SQL
end

# Define BenchJob so deserialization works in Forwarder.
class BenchJob < ActiveJob::Base
  def perform(*); end
end

# ----------------------------------------------------------------------
# Helpers specific to this bench
# ----------------------------------------------------------------------

def truncate_with_adapter!
  ActiveRecord::Base.connection.execute(<<~SQL)
    TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions,
             dispatch_policy_inflight_jobs, dispatch_policy_tick_samples,
             #{ADAPTER_TABLE}
    RESTART IDENTITY CASCADE
  SQL
end

def with_adapter(symbol)
  previous = ActiveJob::Base.queue_adapter_name
  ActiveJob::Base.queue_adapter = symbol
  yield
ensure
  ActiveJob::Base.queue_adapter = previous if previous
end

def adapter_jobs_count
  ActiveRecord::Base.connection
    .execute("SELECT count(*) AS n FROM #{ADAPTER_TABLE}")
    .first["n"].to_i
end

# ----------------------------------------------------------------------
# Benchmarks
# ----------------------------------------------------------------------

Bench.section("Tick.run end-to-end (50 partitions × 5 staged each, 250 admits/tick)") do |sec|
  Bench.register_policy!("bench_real", batch_size: 5)

  # Baseline: TestAdapter so we measure the gem alone.
  ms_test = with_adapter(:test) do
    Bench.measure_median_ms_with_setup(
      runs: 5,
      setup: -> {
        truncate_with_adapter!
        Bench.seed!("bench_real", 50, staged_per_partition: 5)
      },
      work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_real") }
    )
  end
  sec.row("test (gem only, no real INSERT)", ms_test, admits_per_tick: 250)

  # Real adapter run.
  ms_real = with_adapter(ADAPTER_NAME) do
    Bench.measure_median_ms_with_setup(
      runs: 5,
      setup: -> {
        truncate_with_adapter!
        Bench.seed!("bench_real", 50, staged_per_partition: 5)
      },
      work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_real") }
    )
  end
  sec.row(ADAPTER_NAME.to_s, ms_real,
          admits_per_tick: 250,
          adapter_rows: adapter_jobs_count)

  delta_ms = (ms_real - ms_test).round(2)
  delta_pc = ((ms_real - ms_test) / ms_test * 100).round(1)
  sec.row("Δ adapter cost", delta_ms, percent: "#{delta_pc}%")
end

Bench.section("Tick.run scaling: admits per tick (1 partition fan-out)") do |sec|
  [50, 250, 1_000].each do |admits|
    Bench.register_policy!("bench_real_scale", batch_size: admits)
    ms = with_adapter(ADAPTER_NAME) do
      Bench.measure_median_ms_with_setup(
        runs: 3,
        setup: -> {
          truncate_with_adapter!
          Bench.seed!("bench_real_scale", 1, staged_per_partition: admits)
        },
        work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_real_scale") }
      )
    end
    rate = (admits.to_f / (ms / 1000.0)).round
    sec.row("#{admits} admits/tick (1 partition)", ms,
            "jobs/sec end-to-end": rate,
            adapter_rows: adapter_jobs_count)
  end
end

Bench.section("Forwarder.dispatch only (no Tick overhead)") do |sec|
  rows = lambda do |n|
    n.times.map do |i|
      {
        "policy_name"   => "bench_real",
        "partition_key" => "p#{i % 10}",
        "queue_name"    => nil,
        "job_class"     => "BenchJob",
        "job_data"      => {
          "job_id"     => SecureRandom.uuid,
          "job_class"  => "BenchJob",
          "arguments"  => [{}]
        },
        "context"       => {},
        "scheduled_at"  => nil,
        "priority"      => 0,
        "enqueued_at"   => Time.now
      }
    end
  end

  [10, 100, 1_000].each do |n|
    ms = with_adapter(ADAPTER_NAME) do
      Bench.measure_median_ms_with_setup(
        runs: 3,
        setup: -> {
          ActiveRecord::Base.connection.execute("TRUNCATE #{ADAPTER_TABLE} RESTART IDENTITY")
        },
        work:  -> { DispatchPolicy::Forwarder.dispatch(rows.call(n)) }
      )
    end
    rate = (n.to_f / (ms / 1000.0)).round
    sec.row("#{n} immediate jobs into #{ADAPTER_NAME}", ms,
            "jobs/sec": rate,
            adapter_rows: adapter_jobs_count)
  end
end

puts "\n_(adapter: #{ADAPTER_NAME}, db: #{ENV.fetch('DB_NAME')})_"
Bench.print_report
