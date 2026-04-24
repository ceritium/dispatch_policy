# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class TickTest < ActiveSupport::TestCase
    class FairJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
      end
      def perform(*); end
    end

    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    setup do
      @orig_quantum    = DispatchPolicy.config.round_robin_quantum
      @orig_batch_size = DispatchPolicy.config.batch_size
    end

    teardown do
      DispatchPolicy.config.round_robin_quantum = @orig_quantum
      DispatchPolicy.config.batch_size          = @orig_batch_size
    end

    test "each key gets at least quantum when batch_size is tight" do
      DispatchPolicy.config.round_robin_quantum = 3
      DispatchPolicy.config.batch_size          = 9

      20.times { FairJob.perform_later("A") }
      20.times { FairJob.perform_later("B") }
      20.times { FairJob.perform_later("C") }

      Tick.run(policy_name: FairJob.resolved_dispatch_policy.name)
      grouped = StagedJob.admitted.group(:round_robin_key).count

      assert_equal 3, grouped["A"]
      assert_equal 3, grouped["B"]
      assert_equal 3, grouped["C"]
    end

    test "top-up fills unused batch slots beyond quantum for a single tenant" do
      DispatchPolicy.config.round_robin_quantum = 5
      DispatchPolicy.config.batch_size          = 40

      60.times { FairJob.perform_later("only") }
      Tick.run(policy_name: FairJob.resolved_dispatch_policy.name)
      assert_equal 40, StagedJob.admitted.count
    end

    test "concurrency gate caps admissions per partition" do
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")

      Tick.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, StagedJob.admitted.count
      assert_equal 2, StagedJob.pending.count
    end
  end
end
