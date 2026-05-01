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

    # The Proc takes partition_key. We use a class-level Hash as a
    # tiny "lookup table" the test can mutate to simulate plan
    # changes between sync passes.
    class DynamicCappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      class << self
        attr_accessor :plan_table
      end
      self.plan_table = { "pro" => 5, "free" => 1 }

      dispatch_policy do
        partition_by ->(args) { args.first[:account_id] }
        concurrency  max: ->(partition_key) {
          plan = DynamicCappedJob.plan_table.fetch(partition_key, "free")
          plan.is_a?(Integer) ? plan : DynamicCappedJob.plan_table[plan] || 1
        }
      end
      def perform(*); end
    end

    test "dynamic concurrency max resolves per-partition at stage time" do
      DynamicCappedJob.plan_table = { "free-1" => 1, "pro-1" => 5 }
      DynamicCappedJob.perform_later(account_id: "free-1")
      DynamicCappedJob.perform_later(account_id: "pro-1")

      assert_equal 1, PolicyPartition.find_by(partition_key: "free-1").concurrency_max
      assert_equal 5, PolicyPartition.find_by(partition_key: "pro-1").concurrency_max
    end

    test "TickLoop reload re-evaluates the callable for every partition" do
      DynamicCappedJob.plan_table = { "x" => 5 }
      DynamicCappedJob.perform_later(account_id: "x")
      assert_equal 5, PolicyPartition.find_by(partition_key: "x").concurrency_max

      # "Plan" changes — table now returns a different value.
      DynamicCappedJob.plan_table = { "x" => 20 }

      DispatchPolicy::TickLoop.reload_policy_configs!(DynamicCappedJob.resolved_dispatch_policy.name)

      assert_equal 20, PolicyPartition.find_by(partition_key: "x").concurrency_max,
        "callable cap must be re-resolved for every partition on TickLoop boot"
    end

    # ─── DSL change propagation ─────────────────────────────────

    test "TickLoop reload syncs partition rows to current DSL cap" do
      # Stage a job; its partition is seeded with cap=1 (from CappedJob).
      CappedJob.perform_later("acct-x")
      part = PolicyPartition.find_by(policy_name: CappedJob.resolved_dispatch_policy.name, partition_key: "acct-x")
      assert_equal 1, part.concurrency_max

      # Simulate a DSL change: same policy is now declared with max=10.
      original = CappedJob.resolved_dispatch_policy.instance_variable_get(:@concurrency_max)
      CappedJob.resolved_dispatch_policy.instance_variable_set(:@concurrency_max, 10)

      DispatchPolicy::TickLoop.reload_policy_configs!(CappedJob.resolved_dispatch_policy.name)

      assert_equal 10, part.reload.concurrency_max
    ensure
      CappedJob.resolved_dispatch_policy.instance_variable_set(:@concurrency_max, original) if original
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
