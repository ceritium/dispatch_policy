# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class StatsTest < ActiveSupport::TestCase
    class StatsJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context        ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
      end
      def perform(*); end
    end

    test "policy_summary reflects pending / active / drained counts" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      3.times { |i| StatsJob.perform_later("p#{i}") }

      summary = Stats.policy_summary(policy_name)

      assert_equal policy_name, summary[:policy_name]
      assert_equal 3, summary[:pending]
      assert_equal 0, summary[:admitted]
      assert_equal 3, summary[:active_partitions]
      assert_equal 0, summary[:drained_partitions]
      assert_not_nil summary[:oldest_pending_age_seconds]
      assert_includes summary.keys, :stale_partitions_60s
      assert_includes summary.keys, :stale_partitions_300s
    end

    test "policy_summary moves rows to admitted after Tick.run" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      3.times { |i| StatsJob.perform_later("p#{i}") }
      Tick.run(policy_name: policy_name)

      summary = Stats.policy_summary(policy_name)
      assert_equal 0, summary[:pending]
      assert_equal 3, summary[:admitted]
      assert_equal 0, summary[:active_partitions], "all partitions drained after admit"
      assert_equal 3, summary[:drained_partitions]
    end

    test "health flags rotation_lag when an LRU-lagged partition exists" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")

      # Force the partition state to have a very old last_admitted_at,
      # simulating a partition that hasn't seen the LRU cursor in a
      # while AND no gate is at cap (StatsJob has no tracks_inflight
      # gate, so every stale row is LRU-lagged by definition).
      PartitionState.where(policy_name: policy_name).update_all(
        last_admitted_at: 1.hour.ago,
        pending_count:    1
      )

      assert_equal :rotation_lag, Stats.health(stale_threshold_seconds: 60)
    end

    test "health is :ok in steady state" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")
      Tick.run(policy_name: policy_name)
      assert_equal :ok, Stats.health
    end

    test "slo returns the four canonical signals with budgets and verdicts" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      3.times { |i| StatsJob.perform_later("p#{i}") }

      slo = Stats.slo(policy_name,
        latency_budget_seconds:     60,
        fairness_threshold_seconds: 60,
        throughput_window_seconds:  60)

      assert_equal policy_name, slo[:policy_name]

      assert_includes slo[:latency].keys, :seconds
      assert_equal 60, slo[:latency][:budget]
      assert slo[:latency][:ok], "fresh seed should be within latency budget"

      assert_equal 0, slo[:fairness][:stale_partitions]
      assert_equal 60, slo[:fairness][:threshold]
      assert slo[:fairness][:ok]

      assert_kind_of Float, slo[:throughput][:admissions_per_sec]
      assert_equal 60, slo[:throughput][:window_seconds]

      assert_equal 3, slo[:capacity][:active_partitions]
      assert slo[:capacity][:headroom], "3 partitions < default batch_size means headroom"
    end

    test "slo flips latency.ok when oldest pending exceeds budget" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")
      StagedJob.where(policy_name: policy_name).update_all(staged_at: 5.minutes.ago)

      slo = Stats.slo(policy_name, latency_budget_seconds: 60)
      assert_not slo[:latency][:ok], "5min-old pending should breach 60s budget"
      assert_operator slo[:latency][:seconds], :>, 60
    end

    test "slo flips fairness.ok when a partition is stale beyond threshold" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")
      PartitionState.where(policy_name: policy_name).update_all(
        last_admitted_at: 1.hour.ago, pending_count: 1
      )

      slo = Stats.slo(policy_name, fairness_threshold_seconds: 60)
      assert_not slo[:fairness][:ok]
      assert_equal 1, slo[:fairness][:stale_partitions]
    end

    # ─── bottleneck diagnosis ─────────────────────────────────────

    class GatedRoundRobinJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context        ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "bottleneck classifies gate-blocked partition as gate_blocked, not lru_lagged" do
      policy_name = GatedRoundRobinJob.resolved_dispatch_policy.name
      GatedRoundRobinJob.perform_later("a")

      # Fill the concurrency cap for partition "a" so the gate would
      # legitimately reject this row, then age the partition_state so
      # it would otherwise count as stale.
      PartitionInflightCount.increment(
        policy_name:   policy_name,
        gate_name:     "concurrency",
        partition_key: "a"
      )
      PartitionState.where(policy_name: policy_name, partition_key: "a")
                    .update_all(last_admitted_at: 1.hour.ago)

      result = Stats.bottleneck(policy_name, fairness_threshold_seconds: 60)
      assert result[:applicable]
      assert_equal 1, result[:stale_partitions][:gate_blocked]
      assert_equal 0, result[:stale_partitions][:lru_lagged]
      assert_equal :ok, result[:diagnosis], "all stale are gate_blocked → no dispatch_policy bottleneck"
    end

    test "bottleneck classifies LRU-lagged partition correctly" do
      policy_name = GatedRoundRobinJob.resolved_dispatch_policy.name
      3.times { |i| GatedRoundRobinJob.perform_later("p#{i}") }

      # No gate at cap (in_flight = 0 everywhere). Age the partitions.
      PartitionState.where(policy_name: policy_name).update_all(
        last_admitted_at: 1.hour.ago
      )

      result = Stats.bottleneck(policy_name, fairness_threshold_seconds: 60)
      assert_equal 0, result[:stale_partitions][:gate_blocked]
      assert_equal 3, result[:stale_partitions][:lru_lagged]
      assert_equal :rotation_lag, result[:diagnosis]
      assert_includes result[:suggested_knobs], :round_robin_quantum
    end

    class ThrottleRoundRobinJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context        ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
        gate :throttle, rate: 1, per: 60, burst: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "bottleneck classifies throttle-exhausted partition as gate_blocked" do
      policy_name = ThrottleRoundRobinJob.resolved_dispatch_policy.name
      ThrottleRoundRobinJob.perform_later("a")

      ThrottleBucket.lock(
        policy_name: policy_name, gate_name: "throttle",
        partition_key: "a", burst: 1
      ).update_columns(tokens: 0, refilled_at: Time.current)

      PartitionState.where(policy_name: policy_name, partition_key: "a")
                    .update_all(last_admitted_at: 1.hour.ago)

      result = Stats.bottleneck(policy_name, fairness_threshold_seconds: 60)
      assert_equal 1, result[:stale_partitions][:gate_blocked], "tokens<1 means throttle is the limiter"
      assert_equal 0, result[:stale_partitions][:lru_lagged]
    end

    class AdaptiveRoundRobinJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context        ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
        gate :adaptive_concurrency,
             initial_max:   5,
             min:           1,
             target_lag_ms: 100,
             partition_by:  ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "bottleneck classifies adaptive-at-current_max partition as gate_blocked" do
      policy_name = AdaptiveRoundRobinJob.resolved_dispatch_policy.name
      AdaptiveRoundRobinJob.perform_later("a")

      AdaptiveConcurrencyStats.seed!(
        policy_name:   policy_name,
        gate_name:     :adaptive_concurrency,
        partition_key: "a",
        initial_max:   1
      )
      AdaptiveConcurrencyStats.where(
        policy_name: policy_name, partition_key: "a"
      ).update_all(current_max: 1)
      PartitionInflightCount.increment(
        policy_name:   policy_name,
        gate_name:     "adaptive_concurrency",
        partition_key: "a"
      )
      PartitionState.where(policy_name: policy_name, partition_key: "a")
                    .update_all(last_admitted_at: 1.hour.ago)

      result = Stats.bottleneck(policy_name, fairness_threshold_seconds: 60)
      assert_equal 1, result[:stale_partitions][:gate_blocked]
      assert_equal 0, result[:stale_partitions][:lru_lagged]
    end

    test "bottleneck returns applicable: false for non-round_robin policies" do
      class ConcurrencyOnlyJob < ActiveJob::Base
        include DispatchPolicy::Dispatchable
        dispatch_policy do
          gate :concurrency, max: 1, partition_by: ->(_ctx) { "p" }
        end
        def perform(*); end
      end

      policy_name = ConcurrencyOnlyJob.resolved_dispatch_policy.name
      ConcurrencyOnlyJob.perform_later

      result = Stats.bottleneck(policy_name)
      assert_not result[:applicable]
      assert_equal :ok, result[:diagnosis]
    end

    test "health stays :ok when all stale partitions are gate_blocked" do
      policy_name = GatedRoundRobinJob.resolved_dispatch_policy.name
      GatedRoundRobinJob.perform_later("a")

      PartitionInflightCount.increment(
        policy_name:   policy_name,
        gate_name:     "concurrency",
        partition_key: "a"
      )
      PartitionState.where(policy_name: policy_name, partition_key: "a")
                    .update_all(last_admitted_at: 1.hour.ago)

      assert_equal :ok, Stats.health(stale_threshold_seconds: 60),
        "intentional concurrency cap is not a dispatch_policy-tunable issue"
    end

    test "slo[:fairness] reports gate_blocked alongside stale_partitions" do
      policy_name = GatedRoundRobinJob.resolved_dispatch_policy.name
      GatedRoundRobinJob.perform_later("a")

      PartitionInflightCount.increment(
        policy_name:   policy_name,
        gate_name:     "concurrency",
        partition_key: "a"
      )
      PartitionState.where(policy_name: policy_name, partition_key: "a")
                    .update_all(last_admitted_at: 1.hour.ago)

      slo = Stats.slo(policy_name, fairness_threshold_seconds: 60)
      assert_equal 0, slo[:fairness][:stale_partitions], "gate-blocked stale doesn't break fairness"
      assert_equal 1, slo[:fairness][:gate_blocked]
      assert slo[:fairness][:ok]
    end

    test "Tick.run emits tick.dispatch_policy notification with payload" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")
      StatsJob.perform_later("p1")

      events = []
      sub = ActiveSupport::Notifications.subscribe("tick.dispatch_policy") do |*, payload|
        events << payload
      end

      Tick.run(policy_name: policy_name)

      ActiveSupport::Notifications.unsubscribe(sub)

      assert_equal 1, events.size
      payload = events.first
      assert_equal policy_name, payload[:policy_name]
      assert_equal 2, payload[:admitted]
      assert_equal 2, payload[:partitions]
    end

    test "Tick.reap emits reap.dispatch_policy notification with payload" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")
      Tick.run(policy_name: policy_name)
      StagedJob.admitted.update_all(lease_expires_at: 1.hour.ago)

      events = []
      sub = ActiveSupport::Notifications.subscribe("reap.dispatch_policy") do |*, payload|
        events << payload
      end

      Tick.reap

      ActiveSupport::Notifications.unsubscribe(sub)

      assert_equal 1, events.size
      assert_equal 1, events.first[:reaped]
    end
  end
end
