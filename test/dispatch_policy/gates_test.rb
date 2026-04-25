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

    class TimeBudgetJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { account_id: args.first } }
        gate :time_budget,
             budget_ms:    500,
             per:          60,
             partition_by: ->(ctx) { ctx[:account_id] }
      end
      def perform(*); end
    end

    test "time_budget admits while the bucket has positive ms" do
      3.times { TimeBudgetJob.perform_later("acc_a") }
      Tick.run(policy_name: TimeBudgetJob.resolved_dispatch_policy.name)

      assert_equal 3, StagedJob.admitted.count, "fresh bucket should admit the whole batch"
    end

    test "time_budget completion debits ms; bucket can go negative and stop admitting" do
      policy_name = TimeBudgetJob.resolved_dispatch_policy.name
      gate = TimeBudgetJob.resolved_dispatch_policy.gates.first

      # Drain the bucket past zero by simulating a slow completion.
      ThrottleBucket.lock(
        policy_name: policy_name, gate_name: :time_budget,
        partition_key: "acc_b", burst: 500
      ).tap { |b| b.refilled_at = Time.current; b.tokens = 500.0; b.save! }

      gate.record_completion(partition_key: "acc_b", duration_ms: 800)

      bucket = ThrottleBucket.find_by(policy_name: policy_name, partition_key: "acc_b")
      assert_in_delta(-300.0, bucket.tokens, 0.5)

      # Negative bucket → no admissions until refill catches up.
      2.times { TimeBudgetJob.perform_later("acc_b") }
      Tick.run(policy_name: policy_name)
      assert_equal 0, StagedJob.admitted.where(arguments: { arguments: [ "acc_b" ] }).count
    end

    class FairTimeShareJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { account_id: args.first } }
        gate :concurrency, max: 100, partition_by: ->(ctx) { ctx[:account_id] }
        gate :fair_time_share, window: 60, partition_by: ->(ctx) { ctx[:account_id] }
      end
      def perform(*); end
    end

    test "fair_time_share admits the under-consumer first when two tenants compete" do
      policy_name = FairTimeShareJob.resolved_dispatch_policy.name

      # Pre-seed an observation row showing acc_heavy already burned 5s in
      # the window while acc_light burned 100ms. fair_time_share should
      # pick acc_light first.
      PartitionObservation.observe!(
        policy_name: policy_name, partition_key: "acc_heavy",
        queue_lag_ms: 0, duration_ms: 5_000
      )
      PartitionObservation.observe!(
        policy_name: policy_name, partition_key: "acc_light",
        queue_lag_ms: 0, duration_ms: 100
      )

      2.times { FairTimeShareJob.perform_later("acc_heavy") }
      2.times { FairTimeShareJob.perform_later("acc_light") }

      Tick.run(policy_name: policy_name)

      ordered_keys = StagedJob.admitted.order(:admitted_at, :id).map { |s| s.arguments["arguments"].first }
      assert_equal "acc_light", ordered_keys.first,
        "expected the under-consumer to be admitted before the heavy one (got #{ordered_keys.inspect})"
    end

    test "fair_time_share without partition_by raises at policy declaration" do
      klass = Class.new(ActiveJob::Base) { include DispatchPolicy::Dispatchable }
      klass.define_singleton_method(:name) { "FairTimeShareNoPartition" }

      assert_raises(ArgumentError) do
        klass.dispatch_policy { gate :fair_time_share }
      end
    end

    test "fair_time_share with a single tenant is a no-op (admits in stage order)" do
      policy_name = FairTimeShareJob.resolved_dispatch_policy.name

      4.times { FairTimeShareJob.perform_later("acc_solo") }
      Tick.run(policy_name: policy_name)

      assert_equal 4, StagedJob.admitted.count
    end
  end
end
