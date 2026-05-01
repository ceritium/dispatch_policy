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
      assert_nil   part.last_checked_at, "fresh row goes to the front of the cursor"
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

    test "cursor visits gate-blocked partitions but admits zero" do
      # 5 partitions, all at cap=1.
      5.times { |i| CappedJob.perform_later("p#{i}") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 5, StagedJob.admitted.count

      # Stage more demand on the same blocked partitions.
      5.times { |i| CappedJob.perform_later("p#{i}") }
      pre_check = PolicyPartition.where(policy_name: CappedJob.resolved_dispatch_policy.name).pluck(:last_checked_at).compact

      # No admissions, but the cursor should advance — every visited
      # row's last_checked_at moves forward.
      admitted = Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 0, admitted

      post_check = PolicyPartition.where(policy_name: CappedJob.resolved_dispatch_policy.name).pluck(:last_checked_at)
      assert_operator post_check.compact.min, :>=, pre_check.min,
        "cursor must advance on a no-admit visit"
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

    # ─── Dynamic concurrency cap ────────────────────────────────

    class DynamicCappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      # Plan-aware cap: pro accounts get 5 concurrent, others get 1.
      dispatch_policy do
        partition_by ->(args) { args.first[:account_id] }
        concurrency  max: ->(args) {
          args.first[:plan] == "pro" ? 5 : 1
        }
      end
      def perform(*); end
    end

    test "dynamic concurrency max resolves per-partition at stage time" do
      DynamicCappedJob.perform_later(account_id: "free-1", plan: "free")
      DynamicCappedJob.perform_later(account_id: "pro-1",  plan: "pro")

      free = PolicyPartition.find_by(partition_key: "free-1")
      pro  = PolicyPartition.find_by(partition_key: "pro-1")
      assert_equal 1, free.concurrency_max
      assert_equal 5, pro.concurrency_max
    end

    test "dynamic concurrency max persists per partition; subsequent jobs don't change it" do
      DynamicCappedJob.perform_later(account_id: "x", plan: "pro")
      assert_equal 5, PolicyPartition.find_by(partition_key: "x").concurrency_max

      # Even if "plan" changes mid-flight, the persisted cap stays.
      DynamicCappedJob.perform_later(account_id: "x", plan: "free")
      assert_equal 5, PolicyPartition.find_by(partition_key: "x").concurrency_max,
        "first stage wins; partition cap doesn't update on subsequent stages"
    end

    # ─── Cursor lag ─────────────────────────────────────────────

    test "Dispatch.run emits cursor_lag_ms + active_partitions" do
      payloads = []
      sub = ActiveSupport::Notifications.subscribe("tick.dispatch_policy") do |*args|
        payloads << ActiveSupport::Notifications::Event.new(*args).payload.dup
      end

      begin
        # Stage 5 partitions, age them, then run a tick.
        5.times { |i| CappedJob.perform_later("acct-#{i}") }
        PolicyPartition.where(policy_name: CappedJob.resolved_dispatch_policy.name)
          .update_all(last_checked_at: 30.seconds.ago)

        Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      ensure
        ActiveSupport::Notifications.unsubscribe(sub)
      end

      payload = payloads.last
      assert_equal 5, payload[:active_partitions]
      assert_in_delta 30_000, payload[:cursor_lag_ms], 5_000,
        "cursor lag should reflect oldest partition's wait (~30s)"
    end

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
