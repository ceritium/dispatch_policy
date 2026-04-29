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
