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

    class TimeWeightedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }, weight: :time
      end
      def perform(*); end
    end

    test "weight: :time gives bigger quanta to partitions with less consumed time" do
      DispatchPolicy.config.batch_size = 40

      # Heavy partition has 20s of consumed time recently; light has none.
      # Inverse-weighted, light should claim ~99% of the batch_size budget.
      PartitionObservation.observe!(
        policy_name: TimeWeightedJob.resolved_dispatch_policy.name,
        partition_key: "heavy", queue_lag_ms: 0, duration_ms: 20_000
      )

      30.times { TimeWeightedJob.perform_later("heavy") }
      30.times { TimeWeightedJob.perform_later("light") }

      Tick.run(policy_name: TimeWeightedJob.resolved_dispatch_policy.name)

      grouped = StagedJob.admitted.group(:round_robin_key).count
      assert_operator grouped["light"].to_i, :>, grouped["heavy"].to_i,
        "light should be admitted more than heavy when heavy has burned 20s (got #{grouped.inspect})"
    end

    test "weight: :time with a solo tenant fetches up to batch_size" do
      DispatchPolicy.config.batch_size = 25

      40.times { TimeWeightedJob.perform_later("solo") }
      Tick.run(policy_name: TimeWeightedJob.resolved_dispatch_policy.name)

      assert_equal 25, StagedJob.admitted.count
    end

    test "concurrency gate caps admissions per partition" do
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")

      Tick.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, StagedJob.admitted.count
      assert_equal 2, StagedJob.pending.count
    end

    class PoliteFailureJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      class_attribute :should_set_enqueue_error, default: false

      dispatch_policy { gate :concurrency, max: 5, partition_by: ->(_ctx) { "p" } }

      def perform(*); end

      private

      # Simulate an adapter that reports failure via enqueue_error +
      # leaves successfully_enqueued? false. Override raw_enqueue (not
      # _raw_enqueue) so the same stub works on Rails 7.2 (where the
      # adapter call lives in raw_enqueue) and 8.1 (where raw_enqueue
      # wraps _raw_enqueue in callbacks).
      def raw_enqueue
        if self.class.should_set_enqueue_error
          self.enqueue_error = ActiveJob::EnqueueError.new("simulated polite failure")
          return
        end
        super
      end
    end

    test "polite enqueue failure (enqueue_error set, no raise) reverts admission" do
      policy_name = PoliteFailureJob.resolved_dispatch_policy.name
      PoliteFailureJob.perform_later

      PoliteFailureJob.should_set_enqueue_error = true
      begin
        Tick.run(policy_name: policy_name)
      ensure
        PoliteFailureJob.should_set_enqueue_error = false
      end

      staged = StagedJob.find_by!(policy_name: policy_name)
      assert_nil staged.admitted_at,
        "row must be reverted to pending when the adapter declined to enqueue"
      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight),
        "counter must be decremented when admission is reverted"
    end
  end
end
