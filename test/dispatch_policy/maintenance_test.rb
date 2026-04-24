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

    test "revert_admission reverses a failed enqueue" do
      ReapJob.perform_later("Z")
      Tick.run(policy_name: ReapJob.resolved_dispatch_policy.name)
      staged = StagedJob.admitted.last

      Tick.revert_admission(staged)

      assert_nil staged.reload.admitted_at
      count = PartitionInflightCount.fetch_many(
        policy_name: ReapJob.resolved_dispatch_policy.name,
        gate_name: :concurrency,
        partition_keys: [ "Z" ]
      )
      assert_equal 0, count["Z"]
    end
  end
end
