# frozen_string_literal: true

# Shared bootstrap for the dispatch_policy benchmark scripts. Pulls in
# the regular test_helper to get the dummy Rails app, AR connection,
# and migrated schema, then exposes a few helpers for seeding and
# measuring. NOT a Minitest file — the benchmarks are scripts that
# print numbers to stdout, not pass/fail tests.

ENV["RAILS_ENV"] = "test"

# Skip SimpleCov for benchmarks — we don't want coverage instrumentation
# to skew numbers.
ENV["SIMPLECOV_DISABLED"] = "1" unless ENV.key?("SIMPLECOV_DISABLED")

require_relative "../test_helper"

module DispatchPolicy
  module BenchmarkHelpers
    module_function

    TABLES_IN_TRUNCATE_ORDER = %w[
      dispatch_policy_staged_jobs
      dispatch_policy_partition_counts
      dispatch_policy_throttle_buckets
      dispatch_policy_adaptive_concurrency_stats
      dispatch_policy_partition_observations
    ].freeze

    def truncate_all!
      conn = DispatchPolicy::ApplicationRecord.connection
      conn.execute("TRUNCATE TABLE #{TABLES_IN_TRUNCATE_ORDER.join(', ')} RESTART IDENTITY")
      adapter = ActiveJob::Base.queue_adapter
      adapter.enqueued_jobs.clear if adapter.respond_to?(:enqueued_jobs)
    end

    # Bulk-stage `partitions × jobs_per_partition` rows for a given job
    # class. Uses the same code path as production (StagedJob.stage_many!)
    # but in chunks so we don't hold every ActiveJob instance in memory
    # at once for very large N.
    def seed_staged!(job_class:, partitions:, jobs_per_partition:, chunk: 5_000)
      policy = job_class.resolved_dispatch_policy
      total  = 0

      partitions.times.each_slice(chunk / [ jobs_per_partition, 1 ].max) do |slice|
        jobs = []
        slice.each do |i|
          jobs_per_partition.times { jobs << job_class.new("p#{i}") }
        end
        DispatchPolicy::StagedJob.stage_many!(policy: policy, jobs: jobs)
        total += jobs.size
      end

      total
    end

    # Run `block`, returning [wall_ms, sql_count, result]. SQL count
    # excludes SCHEMA queries (introspection done by AR on first
    # contact with a table).
    def measure
      sql_count = 0
      sub = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
        next if payload[:name] == "SCHEMA"
        sql_count += 1
      end

      result = nil
      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = yield
      wall = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

      ActiveSupport::Notifications.unsubscribe(sub)
      [ wall * 1000.0, sql_count, result ]
    end

    # Pretty-print a markdown row with thousands separators.
    def fmt_int(n)
      n.to_i.to_s.reverse.scan(/.{1,3}/).join("_").reverse
    end

    def fmt_ms(n)
      format("%.1f", n)
    end
  end
end
