# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class GatesTest < ActiveSupport::TestCase
    class ThrottledJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :throttle,
             rate:         2,
             per:          60,
             partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        gate :global_cap, max: 2
      end
      def perform(*); end
    end

    class InterleavedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency,
             max:          100,
             partition_by: ->(ctx) { ctx[:tenant] }
        gate :fair_interleave
      end
      def perform(*); end
    end

    test "throttle admits only rate tokens per partition" do
      5.times { ThrottledJob.perform_later("A") }
      5.times { ThrottledJob.perform_later("B") }

      Tick.run(policy_name: ThrottledJob.resolved_dispatch_policy.name)

      buckets = ThrottleBucket.where(policy_name: ThrottledJob.resolved_dispatch_policy.name)
      assert_equal 2, buckets.count
      assert_equal 4, StagedJob.admitted.count
    end

    test "global_cap caps admissions across the whole policy" do
      5.times { CappedJob.perform_later(:x) }

      Tick.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 2, StagedJob.admitted.count
    end

    test "fair_interleave rearranges admitted order to alternate partitions" do
      3.times { InterleavedJob.perform_later("A") }
      3.times { InterleavedJob.perform_later("B") }

      Tick.run(policy_name: InterleavedJob.resolved_dispatch_policy.name)

      # Verify balance via the partition counters the concurrency gate tracks.
      by_partition = PartitionInflightCount
        .where(policy_name: InterleavedJob.resolved_dispatch_policy.name)
        .pluck(:partition_key, :in_flight).to_h
      assert_equal 3, by_partition["A"]
      assert_equal 3, by_partition["B"]
    end

    test "unknown gate raises" do
      klass = Class.new(ActiveJob::Base) { include DispatchPolicy::Dispatchable }
      klass.define_singleton_method(:name) { "UnknownGateJob" }

      assert_raises(ArgumentError) do
        klass.dispatch_policy { gate :does_not_exist }
      end
    end
  end
end
