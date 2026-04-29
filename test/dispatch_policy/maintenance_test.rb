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

    test "prune_drained_partition_states purges drained rows when ratio crosses the threshold" do
      orig_threshold = DispatchPolicy.config.partition_drained_purge_threshold
      orig_min       = DispatchPolicy.config.partition_drained_purge_min_total
      DispatchPolicy.config.partition_drained_purge_threshold = 0.5
      DispatchPolicy.config.partition_drained_purge_min_total = 10

      now = Time.current
      # 8 drained + 2 active = 10 total, drained_ratio = 0.8 ≥ 0.5
      8.times do |i|
        PartitionState.create!(
          policy_name: "p", partition_key: "drained-#{i}",
          pending_count: 0, last_admitted_at: now - 1.minute
        )
      end
      2.times do |i|
        PartitionState.create!(
          policy_name: "p", partition_key: "active-#{i}",
          pending_count: 3, last_admitted_at: nil
        )
      end

      Tick.prune_drained_partition_states

      assert_equal 2, PartitionState.count, "drained rows above threshold must be deleted"
      assert_equal 2, PartitionState.where("pending_count > 0").count
    ensure
      DispatchPolicy.config.partition_drained_purge_threshold = orig_threshold
      DispatchPolicy.config.partition_drained_purge_min_total = orig_min
    end

    test "prune_drained_partition_states is a no-op below min_total even at high ratio" do
      orig_threshold = DispatchPolicy.config.partition_drained_purge_threshold
      orig_min       = DispatchPolicy.config.partition_drained_purge_min_total
      DispatchPolicy.config.partition_drained_purge_threshold = 0.5
      DispatchPolicy.config.partition_drained_purge_min_total = 100

      # 9 drained + 1 active = below min_total — even though
      # drained_ratio = 0.9, the purge skips.
      now = Time.current
      9.times do |i|
        PartitionState.create!(policy_name: "p", partition_key: "drained-#{i}",
          pending_count: 0, last_admitted_at: now - 1.minute)
      end
      PartitionState.create!(policy_name: "p", partition_key: "active",
        pending_count: 1, last_admitted_at: nil)

      Tick.prune_drained_partition_states

      assert_equal 10, PartitionState.count, "below min_total purge must skip"
    ensure
      DispatchPolicy.config.partition_drained_purge_threshold = orig_threshold
      DispatchPolicy.config.partition_drained_purge_min_total = orig_min
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
