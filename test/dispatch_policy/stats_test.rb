# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class StatsTest < ActiveSupport::TestCase
    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        concurrency  max: 1
      end
      def perform(*); end
    end

    test "policy_summary counts ready vs concurrency_blocked" do
      # Saturate one partition.
      2.times { CappedJob.perform_later("a") }
      Dispatch.run(policy_name: CappedJob.resolved_dispatch_policy.name)

      # Add a fresh partition (ready).
      CappedJob.perform_later("b")

      s = Stats.policy_summary(CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, s[:ready_partitions]
      assert_equal 1, s[:concurrency_blocked]
    end

    test "health == :ok when no expired leases" do
      assert_equal :ok, Stats.health
    end

    test "bottleneck reports :ok when no demand" do
      result = Stats.bottleneck(CappedJob.resolved_dispatch_policy.name)
      assert_equal :ok, result[:diagnosis]
    end

    test "bottleneck reports :capacity_strain when ready > batch_size" do
      DispatchPolicy.config.batch_size = 2
      4.times { |i| CappedJob.perform_later("acct-#{i}") }

      result = Stats.bottleneck(CappedJob.resolved_dispatch_policy.name)
      assert_equal :capacity_strain, result[:diagnosis]
      assert_equal 2,   result[:recommended_config][:batch_size][:current]
      assert_equal 100, result[:recommended_config][:batch_size][:suggested]
    end

    test "tick_runs returns zeros when no samples" do
      assert_equal 0, Stats.tick_runs(window: 60)[:ticks]
    end
  end
end
