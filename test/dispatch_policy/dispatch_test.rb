# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class DispatchTest < ActiveSupport::TestCase
    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        concurrency  max: 1
      end
      def perform(*); end
    end

    class ThrottledJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        # 1 token / sec, burst 2 — admits up to 2 immediately, then drips.
        throttle rate: 1, per: 1.second, burst: 2
      end
      def perform(*); end
    end

    class PlainJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy { }
      def perform(*); end
    end

    setup { DispatchPolicy.config.batch_size = 50 }

    # ─── Stage path ─────────────────────────────────────────────

    test "stage! creates StagedJob and seeds PolicyPartition" do
      assert_difference [ "StagedJob.count", "PolicyPartition.count" ], 1 do
        CappedJob.perform_later("a")
      end

      part = PolicyPartition.find_by(policy_name: CappedJob.resolved_dispatch_policy.name, partition_key: "a")
      assert_equal 1, part.pending_count
      assert_equal 1, part.concurrency_max
      assert       part.ready
    end

    # ─── Concurrency cap ────────────────────────────────────────

    test "concurrency cap admits up to max per partition" do
      3.times { CappedJob.perform_later("acct-a") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, StagedJob.admitted.count
      assert_equal 2, StagedJob.pending.count
    end

    test "blocked partition does not stop other partitions from admitting" do
      # acct-a: 1 in flight already (admit it), 5 pending behind.
      5.times { CappedJob.perform_later("acct-a") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, StagedJob.admitted.count

      # acct-b enters fresh while acct-a is blocked at cap.
      3.times { CappedJob.perform_later("acct-b") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)

      # acct-a still 1, acct-b admitted 1 (its cap is also 1).
      grouped = StagedJob.admitted.group(:partition_key).count
      assert_equal 1, grouped["acct-a"]
      assert_equal 1, grouped["acct-b"]
    end

    test "ready queue excludes at-cap partitions" do
      # 5 partitions, all at cap=1.
      5.times { |i| CappedJob.perform_later("p#{i}") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 5, StagedJob.admitted.count

      # Stage more demand on the same blocked partitions.
      5.times { |i| CappedJob.perform_later("p#{i}") }

      # No partition is ready — admit should be a no-op.
      admitted = Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 0, admitted
    end

    # ─── Throttle ───────────────────────────────────────────────

    test "throttle bucket caps admission per partition" do
      10.times { ThrottledJob.perform_later("acct-a") }
      Dispatch.run(policy_name: ThrottledJob.resolved_dispatch_policy.name)
      assert_equal 2, StagedJob.admitted.count, "burst=2 → 2 admitted in first tick"
    end

    test "throttle blocked partition leaves others admissible" do
      # Drain acct-a's bucket.
      5.times { ThrottledJob.perform_later("acct-a") }
      Dispatch.run(policy_name: ThrottledJob.resolved_dispatch_policy.name)
      assert_equal 2, StagedJob.admitted.count

      # acct-b has its own bucket, full.
      5.times { ThrottledJob.perform_later("acct-b") }
      Dispatch.run(policy_name: ThrottledJob.resolved_dispatch_policy.name)
      grouped = StagedJob.admitted.group(:partition_key).count
      assert_equal 2, grouped["acct-a"]
      assert_equal 2, grouped["acct-b"]
    end

    # ─── Plain (no partitioning) ────────────────────────────────

    test "plain policy admits everything up to batch_size" do
      DispatchPolicy.config.batch_size = 5
      10.times { PlainJob.perform_later }
      Dispatch.run(policy_name: PlainJob.resolved_dispatch_policy.name)
      assert_equal 5, StagedJob.admitted.count
    end

    # ─── Reap ───────────────────────────────────────────────────

    test "reap completes expired leases and decrements in_flight" do
      DispatchPolicy.config.lease_duration = 1
      CappedJob.perform_later("a")
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      part = PolicyPartition.find_by(partition_key: "a")
      assert_equal 1, part.in_flight

      # Force lease expiry
      StagedJob.update_all(lease_expires_at: 1.minute.ago)
      reaped = Dispatch.reap
      assert_equal 1, reaped

      assert_equal 0, part.reload.in_flight
      assert_equal 1, StagedJob.completed.count
    end
  end
end
