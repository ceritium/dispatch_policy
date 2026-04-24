# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class AdaptiveConcurrencyTest < ActiveSupport::TestCase
    class AdaptiveJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :adaptive_concurrency,
             partition_by:   ->(ctx) { ctx[:tenant] },
             initial_max:    5,
             min:            1,
             target_latency: 200,
             ewma_alpha:     0.5
      end
      def perform(*); end
    end

    def policy_name
      AdaptiveJob.resolved_dispatch_policy.name
    end

    def stats_for(partition)
      AdaptiveConcurrencyStats.find_by(
        policy_name:   policy_name,
        gate_name:     "adaptive_concurrency",
        partition_key: partition
      )
    end

    def record(partition, duration_ms:, succeeded: true)
      gate = AdaptiveJob.resolved_dispatch_policy.gates.first
      gate.record_observation(
        partition_key: partition,
        duration_ms:   duration_ms,
        succeeded:     succeeded
      )
    end

    test "filter seeds stats with initial_max and admits up to that cap" do
      7.times { AdaptiveJob.perform_later("A") }
      Tick.run(policy_name: policy_name)

      # initial_max=5, so 5 admitted.
      assert_equal 5, StagedJob.admitted.count
      assert_equal 5, stats_for("A").current_max
    end

    test "current_max shrinks on slow observations" do
      record("A", duration_ms: 800, succeeded: true)
      record("A", duration_ms: 800, succeeded: true)
      assert stats_for("A").current_max < 5,
        "expected shrink, got #{stats_for('A').current_max}"
    end

    test "current_max shrinks harder on failures" do
      AdaptiveConcurrencyStats.seed!(
        policy_name: policy_name, gate_name: "adaptive_concurrency",
        partition_key: "A", initial_max: 10
      )
      record("A", duration_ms: 100, succeeded: false)
      assert_operator stats_for("A").current_max, :<=, 5
    end

    test "current_max grows unbounded on fast successes" do
      AdaptiveConcurrencyStats.seed!(
        policy_name: policy_name, gate_name: "adaptive_concurrency",
        partition_key: "A", initial_max: 3
      )
      20.times { record("A", duration_ms: 50, succeeded: true) }
      # +1 per fast success; nothing caps it but target_latency.
      assert_equal 23, stats_for("A").current_max
    end

    test "around_perform records duration + success for adaptive gates" do
      AdaptiveJob.perform_later("A")
      Tick.run(policy_name: policy_name)
      staged = StagedJob.admitted.last

      job = ActiveJob::Base.deserialize(staged.arguments)
      job._dispatch_partitions  = staged.partitions
      job._dispatch_admitted_at = staged.admitted_at
      job.perform_now

      stats = stats_for("A")
      assert_equal 1, stats.sample_count
      assert_not_nil stats.last_observed_at
    end

    test "duration measures admitted_at → completion (captures queue wait)" do
      AdaptiveJob.perform_later("A")
      Tick.run(policy_name: policy_name)
      staged = StagedJob.admitted.last

      # Simulate the staged row having been admitted 2 seconds ago — as
      # happens under queue saturation when workers are busy.
      two_sec_ago = 2.seconds.ago
      staged.update!(admitted_at: two_sec_ago)

      job = ActiveJob::Base.deserialize(staged.arguments)
      job._dispatch_partitions  = staged.partitions
      job._dispatch_admitted_at = two_sec_ago
      job.perform_now

      stats = stats_for("A")
      # EWMA of a single observation ≈ duration * alpha. With alpha=0.5 and
      # duration ≥ 2000ms, EWMA should be ≥ ~1000ms — well above target=200ms.
      assert_operator stats.ewma_latency_ms, :>=, 900,
        "expected queue-wait latency reflected, got #{stats.ewma_latency_ms}"
      assert_operator stats.current_max, :<=, 5,
        "expected cap to shrink or stay put, got #{stats.current_max}"
    end
  end
end
