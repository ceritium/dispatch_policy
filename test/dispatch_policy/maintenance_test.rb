# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class MaintenanceTest < ActiveSupport::TestCase
    class ReapJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "reap releases counters and completes rows with expired leases" do
      ReapJob.perform_later("A")
      Tick.run(policy_name: ReapJob.resolved_dispatch_policy.name)
      staged = StagedJob.admitted.last
      assert_not_nil staged

      # Force the lease into the past.
      staged.update_columns(lease_expires_at: 1.hour.ago)

      Tick.reap

      assert_not_nil staged.reload.completed_at
      count = PartitionInflightCount.fetch_many(
        policy_name: ReapJob.resolved_dispatch_policy.name,
        gate_name: :concurrency,
        partition_keys: [ "A" ]
      )
      assert_equal 0, count["A"]
    end

    test "reap issues a constant number of SQL statements regardless of expired-row count" do
      # Architectural assertion: Tick.reap must be O(1) in SQL
      # round-trips, not O(expired). A regression that loops one
      # update per row will fire ~2 statements per row and trip this.
      policy_name = ReapJob.resolved_dispatch_policy.name

      40.times { |i| ReapJob.perform_later("p#{i}") }
      Tick.run(policy_name: policy_name)
      StagedJob.admitted.update_all(lease_expires_at: 1.hour.ago)

      reap_writes = []
      callback = ->(_, _, _, _, payload) {
        sql = payload[:sql]
        next if payload[:name] == "SCHEMA"
        next unless /\A\s*UPDATE/i.match?(sql)
        next unless sql.include?("dispatch_policy_staged_jobs") ||
                    sql.include?("dispatch_policy_partition_counts")
        reap_writes << sql
      }

      ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
        Tick.reap
      end

      # Expected: 1 UPDATE … RETURNING on staged_jobs + 1 UPDATE FROM
      # (VALUES …) on partition_counts. Cap at 4 to leave headroom.
      assert_operator reap_writes.size, :<=, 4,
        "reap must batch its updates, got #{reap_writes.size} writes for 40 expired rows:\n" \
        "#{reap_writes.join("\n")}"
    end

    test "prune_orphan_gate_rows removes rows for unknown policies" do
      PartitionInflightCount.increment(
        policy_name: "ghost_policy", gate_name: "concurrency", partition_key: "x"
      )
      assert_equal 1, PartitionInflightCount.where(policy_name: "ghost_policy").count

      Tick.prune_orphan_gate_rows
      assert_equal 0, PartitionInflightCount.where(policy_name: "ghost_policy").count
    end

    test "prune_idle_partitions drops stale empty buckets" do
      bucket = ThrottleBucket.lock(
        policy_name: "any", gate_name: "throttle", partition_key: "k", burst: 1
      )
      bucket.update_columns(tokens: 0, refilled_at: 1.hour.ago)

      orig_ttl = DispatchPolicy.config.partition_idle_ttl
      DispatchPolicy.config.partition_idle_ttl = 1
      begin
        Tick.prune_idle_partitions
      ensure
        DispatchPolicy.config.partition_idle_ttl = orig_ttl
      end

      assert_equal 0, ThrottleBucket.where(policy_name: "any").count
    end
  end
end
