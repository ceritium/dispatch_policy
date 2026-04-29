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

    test "health flags starvation when active partition has stale last_admitted_at" do
      policy_name = StatsJob.resolved_dispatch_policy.name
      StatsJob.perform_later("p0")

      # Force the partition state to have a very old last_admitted_at,
      # simulating a partition that hasn't seen the LRU cursor in a
      # while.
      PartitionState.where(policy_name: policy_name).update_all(
        last_admitted_at: 1.hour.ago,
        pending_count:    1
      )

      assert_equal :starvation, Stats.health(stale_threshold_seconds: 60)
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
