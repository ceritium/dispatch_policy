# frozen_string_literal: true

# Push the gem past comfortable scales to find where things break:
#
#   - claim_partitions @ 100k partitions in DB
#   - claim_staged_jobs! @ 50k staged in one partition
#   - Forwarder.dispatch @ 5k jobs into good_job in one call
#   - Tick.run with admission_batch_size = 1000, 5000
#   - Concurrent stage_many! from N threads
#
# These are not "what we expect to run in production" — they're the
# upper-bound numbers, so we know how far the gem stretches before
# something material changes shape (TX wall time, PG bind-param limit,
# memory).
#
# Run against the dummy DB so good_jobs is available:
#
#   BENCH_DB_NAME=dispatch_policy_dummy bundle exec rake bench:limits
#
# Or without good_job for the gem-only ceilings:
#
#   bundle exec rake bench:limits

ENV["DB_NAME"]       ||= ENV["BENCH_DB_NAME"] || "dispatch_policy_bench"
ENV["DUMMY_ADAPTER"] ||= ENV["BENCH_ADAPTER"] || "good_job"

# Boot the dummy app only if we plan to use good_job; otherwise stay
# light and use TestAdapter.
USE_GOOD_JOB = ENV["BENCH_DB_NAME"] == "dispatch_policy_dummy" || ENV["BENCH_ADAPTER"] == "good_job"
if USE_GOOD_JOB
  ENV["RAILS_ENV"]     ||= "development"
  ENV["DATABASE_URL"]  ||= "postgres://#{ENV.fetch('DB_USER', ENV['USER'])}@" \
                          "#{ENV.fetch('DB_HOST', 'localhost')}/#{ENV['DB_NAME']}"
  require File.expand_path("../dummy/config/environment", __dir__)
end

require_relative "bench_helper"
Bench.connect!

# The concurrent section spawns up to 8 threads doing stage_many — each
# wants its own AR connection. Reconnect with a pool that fits.
ActiveRecord::Base.establish_connection(
  ActiveRecord::Base.connection_db_config.configuration_hash.merge(pool: 16)
)

# Make sure the schema exists. If not, recreate it (won't have good_job
# tables but limits we care about are gem-internal).
unless ActiveRecord::Base.connection.table_exists?("dispatch_policy_partitions")
  Bench.recreate_schema!
end

# Make sure fairness columns exist (in case we're on an old dummy DB).
gem_cols = ActiveRecord::Base.connection.columns("dispatch_policy_partitions").map(&:name)
unless gem_cols.include?("decayed_admits")
  ActiveRecord::Base.connection.execute(<<~SQL)
    ALTER TABLE dispatch_policy_partitions
      ADD COLUMN IF NOT EXISTS decayed_admits double precision NOT NULL DEFAULT 0.0,
      ADD COLUMN IF NOT EXISTS decayed_admits_at timestamp
  SQL
end

class BenchJob < ActiveJob::Base
  def perform(*); end
end

# AR models for tick samples write
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

ADAPTER_TABLE = USE_GOOD_JOB ? "good_jobs" : nil

def truncate_all!
  tables = "dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
           "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples"
  tables += ", #{ADAPTER_TABLE}" if USE_GOOD_JOB && ActiveRecord::Base.connection.table_exists?(ADAPTER_TABLE)
  ActiveRecord::Base.connection.execute("TRUNCATE #{tables} RESTART IDENTITY CASCADE")
end

def with_adapter(symbol)
  previous = ActiveJob::Base.queue_adapter_name
  ActiveJob::Base.queue_adapter = symbol
  yield
ensure
  ActiveJob::Base.queue_adapter = previous if previous
end

# ----------------------------------------------------------------------
# claim_partitions ceiling
# ----------------------------------------------------------------------

Bench.section("claim_partitions: how many partitions before the SELECT degrades?") do |sec|
  [10_000, 50_000, 100_000].each do |total|
    truncate_all!
    Bench.seed!("bench_limit_claim", total, staged_per_partition: 1)
    median = Bench.measure_median_ms_with_setup(
      runs: 3,
      setup: -> {
        # Reset last_checked_at so each run sees a clean ordering.
        ActiveRecord::Base.connection.execute(
          "UPDATE dispatch_policy_partitions SET last_checked_at = NULL WHERE policy_name = 'bench_limit_claim'"
        )
      },
      work:  -> {
        ActiveRecord::Base.connection.transaction(requires_new: true) do
          DispatchPolicy::Repository.claim_partitions(policy_name: "bench_limit_claim", limit: 50)
          raise ActiveRecord::Rollback
        end
      }
    )
    sec.row("#{total} partitions in DB", median, partitions_returned: 50)
  end
end

# ----------------------------------------------------------------------
# claim_staged_jobs!: huge backlog in one partition
# ----------------------------------------------------------------------

Bench.section("claim_staged_jobs! with deep backlog (1 partition)") do |sec|
  [1_000, 10_000, 50_000].each do |pending|
    truncate_all!
    Bench.seed!("bench_limit_claim_staged", 1, staged_per_partition: pending)
    median = Bench.measure_median_ms_with_setup(
      runs: 3,
      setup: -> {
        truncate_all!
        Bench.seed!("bench_limit_claim_staged", 1, staged_per_partition: pending)
      },
      work:  -> {
        ActiveRecord::Base.connection.transaction(requires_new: true) do
          DispatchPolicy::Repository.claim_staged_jobs!(
            policy_name:       "bench_limit_claim_staged",
            partition_key:     "p0",
            limit:             100,
            gate_state_patch:  {},
            retry_after:       nil,
            half_life_seconds: 60
          )
        end
      }
    )
    sec.row("#{pending} staged in 1 partition, claim 100", median)
  end
end

# ----------------------------------------------------------------------
# Forwarder.dispatch ceiling against good_job
# ----------------------------------------------------------------------

if USE_GOOD_JOB && ActiveRecord::Base.connection.table_exists?("good_jobs")
  Bench.section("Forwarder.dispatch into good_job: batch size limit") do |sec|
    [100, 1_000, 5_000].each do |n|
      rows = n.times.map do |i|
        {
          "policy_name"   => "bench_limit_fwd",
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

      ms = with_adapter(:good_job) do
        Bench.measure_median_ms_with_setup(
          runs: 3,
          setup: -> {
            ActiveRecord::Base.connection.execute("TRUNCATE good_jobs RESTART IDENTITY")
          },
          work:  -> { DispatchPolicy::Forwarder.dispatch(rows) }
        )
      end
      rate = (n.to_f / (ms / 1000.0)).round
      sec.row("#{n} jobs in 1 dispatch call", ms, "jobs/sec": rate)
    end
  end

  # Tick.run with very large admission_batch_size — what's the ceiling
  # for how many jobs we can admit in a SINGLE tick, end-to-end?
  Bench.section("Tick.run: admission_batch_size ceiling (1 partition, good_job)") do |sec|
    [1_000, 5_000, 10_000].each do |admits|
      Bench.register_policy!("bench_limit_tick", batch_size: admits)
      ms = with_adapter(:good_job) do
        Bench.measure_median_ms_with_setup(
          runs: 3,
          setup: -> {
            truncate_all!
            Bench.seed!("bench_limit_tick", 1, staged_per_partition: admits)
          },
          work:  -> { DispatchPolicy::Tick.run(policy_name: "bench_limit_tick") }
        )
      end
      rate = (admits.to_f / (ms / 1000.0)).round
      seconds = (ms / 1000.0).round(2)
      sec.row("#{admits} admits/tick (1 partition)", ms,
              "jobs/sec": rate,
              wall: "#{seconds}s")
    end
  end
end

# ----------------------------------------------------------------------
# Concurrent stage_many! — what does contention look like?
# ----------------------------------------------------------------------

Bench.section("Concurrent stage_many! from N threads (5k rows each, 100 partitions)") do |sec|
  [1, 4, 8].each do |n_threads|
    rows_per_thread = 5_000.times.map do |i|
      {
        policy_name:   "bench_limit_concurrent",
        partition_key: "p#{i % 100}",
        queue_name:    nil,
        shard:         "default",
        job_class:     "BenchJob",
        job_data:      { "job_id" => SecureRandom.uuid, "job_class" => "BenchJob", "arguments" => [{}] },
        context:       {},
        scheduled_at:  nil,
        priority:      0
      }
    end

    median = Bench.measure_median_ms_with_setup(
      runs: 3,
      setup: -> { truncate_all! },
      work:  -> {
        threads = n_threads.times.map do
          Thread.new do
            ActiveRecord::Base.connection_pool.with_connection do
              DispatchPolicy::Repository.stage_many!(rows_per_thread)
            end
          end
        end
        threads.each(&:join)
      }
    )
    total_rows = n_threads * 5_000
    rate = (total_rows.to_f / (median / 1000.0)).round
    sec.row("#{n_threads} thread(s) × 5k rows", median,
            "total rows": total_rows, "jobs/sec": rate)
  end
end

puts "\n_(adapter: #{USE_GOOD_JOB ? 'good_job' : 'test'}, db: #{ENV.fetch('DB_NAME')})_"
Bench.print_report
