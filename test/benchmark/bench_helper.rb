# frozen_string_literal: true

# Shared helpers for the manual benchmark scripts. Not part of the
# regular test suite — these talk to a real Postgres, take seconds to
# minutes per run, and print a Markdown report on stdout.
#
# Usage from a benchmark file:
#
#   require_relative "bench_helper"
#   Bench.connect!
#   Bench.recreate_schema!
#   Bench.section("Tick.run") do |s|
#     s.row("100 partitions",  Bench.measure_ms { ... })
#     s.row("1k partitions",   Bench.measure_ms { ... })
#   end
#   Bench.print_report

require "json"
require "securerandom"
require "active_record"
require "active_job"
ActiveJob::Base.queue_adapter = :test

require_relative "../../lib/dispatch_policy"

module Bench
  module_function

  REPORT = []

  def connect!
    return if @connected

    ActiveRecord::Base.establish_connection(
      adapter:  "postgresql",
      encoding: "unicode",
      host:     ENV.fetch("DB_HOST", "localhost"),
      username: ENV.fetch("DB_USER", ENV["USER"]),
      password: ENV.fetch("DB_PASS", ""),
      database: ENV.fetch("DB_NAME", "dispatch_policy_bench")
    )
    ActiveRecord::Base.connection.execute("SELECT 1")
    @connected = true
  rescue ActiveRecord::NoDatabaseError
    abort <<~ERR
      Database #{ENV.fetch('DB_NAME', 'dispatch_policy_bench')} does not exist.
      Create it first:
        createdb #{ENV.fetch('DB_NAME', 'dispatch_policy_bench')}
    ERR
  end

  def recreate_schema!
    require_relative "../../db/migrate/20260501000001_create_dispatch_policy_tables"
    ActiveRecord::Base.connection.execute(
      "DROP TABLE IF EXISTS dispatch_policy_staged_jobs, " \
      "dispatch_policy_partitions, dispatch_policy_inflight_jobs, " \
      "dispatch_policy_tick_samples CASCADE"
    )
    ActiveRecord::Migration.suppress_messages do
      CreateDispatchPolicyTables.new.change
    end
  end

  def truncate!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )
  end

  # Bulk-seed N partitions, each with `staged_per_partition` rows in
  # staged_jobs. Uses raw multi-row INSERTs so 10k partitions × 10
  # staged rows = 100k inserts complete in O(seconds) on local PG.
  def seed!(policy_name, n_partitions, staged_per_partition: 5)
    conn = ActiveRecord::Base.connection

    # Partitions: one INSERT with N tuples.
    part_values = []
    n_partitions.times do |i|
      part_values << "('#{policy_name}', 'p#{i}', NULL, 'default', 'active', " \
                     "#{staged_per_partition}, 0, '{}'::jsonb, NULL, NULL, NULL, NULL, " \
                     "NULL, '{}'::jsonb, 0.0, NULL, now(), now())"
    end
    part_values.each_slice(2_000) do |chunk|
      conn.execute(<<~SQL)
        INSERT INTO dispatch_policy_partitions
          (policy_name, partition_key, queue_name, shard, status,
           pending_count, total_admitted, context, context_updated_at,
           last_enqueued_at, last_checked_at, last_admit_at,
           next_eligible_at, gate_state,
           decayed_admits, decayed_admits_at, created_at, updated_at)
        VALUES #{chunk.join(", ")}
      SQL
    end

    # Staged: N × staged_per_partition rows. job_id MUST be a real UUID
    # — good_job stores it on a uuid-typed column, and a non-uuid value
    # is silently dropped to NULL on insert (which then fails the
    # successfully_enqueued? check Forwarder uses).
    staged_values = []
    n_partitions.times do |i|
      staged_per_partition.times do |_j|
        job_id = SecureRandom.uuid
        job_data = {
          job_id:     job_id,
          job_class:  "BenchJob",
          arguments:  [{}]
        }.to_json
        staged_values << "('#{policy_name}', 'p#{i}', NULL, 'BenchJob', " \
                         "'#{job_data.gsub("'", "''")}'::jsonb, '{}'::jsonb, NULL, 0, now())"
      end
    end
    staged_values.each_slice(2_000) do |chunk|
      conn.execute(<<~SQL)
        INSERT INTO dispatch_policy_staged_jobs
          (policy_name, partition_key, queue_name, job_class, job_data,
           context, scheduled_at, priority, enqueued_at)
        VALUES #{chunk.join(", ")}
      SQL
    end
  end

  # Register a policy in-memory so Tick can find it.
  def register_policy!(policy_name, batch_size: 100, half_life: nil, tick_cap: nil)
    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build(policy_name) do
      context ->(_args) { {} }
      gate :throttle, rate: 1_000_000, per: 60, partition_by: ->(_c) { "k" }
      admission_batch_size batch_size
      fairness half_life: half_life if half_life
      tick_admission_budget tick_cap if tick_cap
    end
    DispatchPolicy.registry.register(policy)
    policy
  end

  # Wall-clock measurement in ms (monotonic).
  def measure_ms
    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1_000).round(2)
  end

  # Median over N runs, with the first run discarded as warmup.
  def measure_median_ms(runs: 5, &block)
    samples = runs.times.map { measure_ms(&block) }
    samples.shift # warmup
    return samples.first if samples.size == 1
    sorted = samples.sort
    sorted[sorted.size / 2]
  end

  # Same as measure_median_ms but lets the caller pass a setup proc
  # that runs OUTSIDE the timed block (e.g. truncate+seed). Critical
  # for benchmarks where each iteration consumes the seeded data and
  # the next iteration would otherwise measure "tick with nothing to
  # do".
  def measure_median_ms_with_setup(runs: 5, setup:, work:)
    samples = runs.times.map do
      setup.call
      measure_ms { work.call }
    end
    samples.shift
    return samples.first if samples.size == 1
    sorted = samples.sort
    sorted[sorted.size / 2]
  end

  # ---- report sections -----------------------------------------------------

  Section = Struct.new(:name, :rows)
  Row     = Struct.new(:label, :ms, :extras)

  def section(name)
    sec = Section.new(name, [])
    helper = SectionHelper.new(sec)
    yield helper
    REPORT << sec
  end

  class SectionHelper
    def initialize(section); @section = section; end
    def row(label, ms, extras = {})
      @section.rows << Row.new(label, ms, extras)
    end
  end

  def print_report
    REPORT.each do |sec|
      puts "\n## #{sec.name}\n"
      extra_keys = sec.rows.flat_map { |r| r.extras.keys }.uniq
      headers = ["scenario", "ms"] + extra_keys.map(&:to_s)
      puts "| #{headers.join(' | ')} |"
      puts "|#{headers.map { |_| '---' }.join('|')}|"
      sec.rows.each do |r|
        cells = [r.label, format("%.2f", r.ms)] + extra_keys.map { |k| r.extras[k].to_s }
        puts "| #{cells.join(' | ')} |"
      end
    end
    puts "\n_(median of #{ENV.fetch('RUNS', '5')} runs, first discarded as warmup)_"
    # Clear so run_all.rb can `load` the next script without double-printing
    # accumulated sections from the previous one.
    REPORT.clear
  end
end
