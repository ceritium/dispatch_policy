# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  # Tests that exercise non-happy-path behaviour: gate failures during
  # admission, post-admission enqueue failures, dedupe state transitions,
  # reaper / completion races, and adaptive-cap floor enforcement.
  class ResilienceTest < ActiveSupport::TestCase
    # ───── Group A: Tick.run failure paths ─────

    class RaisingGate < DispatchPolicy::Gate
      def filter(_batch, _context)
        raise "intentional gate failure"
      end
    end
    Gate.register(:raising, RaisingGate)

    class RaisingGateJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy { gate :raising }
      def perform(*); end
    end

    test "a gate that raises during filter aborts the tick without admitting anything" do
      3.times { RaisingGateJob.perform_later }
      assert_raises(RuntimeError) do
        Tick.run(policy_name: RaisingGateJob.resolved_dispatch_policy.name)
      end

      # Transaction rolled back: no admissions, all rows still pending,
      # no counters incremented.
      scope = StagedJob.where(policy_name: RaisingGateJob.resolved_dispatch_policy.name)
      assert_equal 3, scope.pending.count
      assert_equal 0, scope.admitted.count
      assert_equal 0, PartitionInflightCount
        .where(policy_name: RaisingGateJob.resolved_dispatch_policy.name).sum(:in_flight)
    end

    class FailingEnqueueJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      class_attribute :enqueue_should_raise, default: false

      dispatch_policy do
        gate :concurrency, max: 5, partition_by: ->(_ctx) { "p" }
      end

      def perform(*); end

      private

      # Override raw_enqueue (not _raw_enqueue) so the stub catches
      # the adapter call on both Rails 7.2 (raw_enqueue holds the
      # adapter call itself) and 8.1 (raw_enqueue runs callbacks +
      # delegates to _raw_enqueue).
      def raw_enqueue
        raise "simulated adapter failure" if self.class.enqueue_should_raise
        super
      end
    end

    test "adapter raise during enqueue rolls back the entire staging transaction" do
      policy_name = FailingEnqueueJob.resolved_dispatch_policy.name
      FailingEnqueueJob.perform_later

      FailingEnqueueJob.enqueue_should_raise = true
      begin
        # The exception bubbles out of Tick.run — the surrounding
        # TickLoop catches it; here we assert the rollback behaviour.
        assert_raises(RuntimeError) { Tick.run(policy_name: policy_name) }
      ensure
        FailingEnqueueJob.enqueue_should_raise = false
      end

      staged = StagedJob.find_by!(policy_name: policy_name)
      assert_nil staged.admitted_at, "TX rollback must leave the row pending"
      assert_nil staged.active_job_id
      assert_empty staged.partitions
      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight),
        "counter increments must roll back with the rest of the TX"
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

    test "polite adapter decline (enqueue_error set, no raise) rolls back the staging transaction" do
      policy_name = PoliteFailureJob.resolved_dispatch_policy.name
      PoliteFailureJob.perform_later

      PoliteFailureJob.should_set_enqueue_error = true
      begin
        # Polite decline is internally raised as EnqueueDeclined and
        # rescued inside Tick.run, so this returns 0 instead of bubbling.
        assert_equal 0, Tick.run(policy_name: policy_name)
      ensure
        PoliteFailureJob.should_set_enqueue_error = false
      end

      staged = StagedJob.find_by!(policy_name: policy_name)
      assert_nil staged.admitted_at, "TX rollback must leave the row pending"
      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight)
    end

    class BatchFailingJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      class_attribute :fail_on_index, default: nil
      class_attribute :enqueue_call_count, default: 0

      dispatch_policy { gate :concurrency, max: 100, partition_by: ->(_ctx) { "b" } }

      def perform(*); end

      private

      def raw_enqueue
        idx = self.class.enqueue_call_count
        self.class.enqueue_call_count = idx + 1
        raise "simulated mid-batch failure" if idx == self.class.fail_on_index
        super
      end
    end

    test "raise mid-batch rolls back every prior admission in the same tick" do
      policy_name = BatchFailingJob.resolved_dispatch_policy.name
      3.times { BatchFailingJob.perform_later }

      BatchFailingJob.fail_on_index       = 1   # second enqueue raises
      BatchFailingJob.enqueue_call_count  = 0
      begin
        assert_raises(RuntimeError) { Tick.run(policy_name: policy_name) }
      ensure
        BatchFailingJob.fail_on_index      = nil
        BatchFailingJob.enqueue_call_count = 0
      end

      scope = StagedJob.where(policy_name: policy_name)
      assert_equal 3, scope.pending.count, "all three rows revert when one mid-batch enqueue raises"
      assert_equal 0, scope.admitted.count
      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight),
        "counter increments from earlier successful enqueues in the batch must roll back too"
    end

    # ───── Group B: dedupe state transitions ─────

    class DedupeJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { id: args.first } }
        dedupe_key ->(args) { "dedupe:#{args.first}" }
      end
      def perform(*); end
    end

    test "dedupe drops a re-stage while the previous row is still admitted" do
      policy_name = DedupeJob.resolved_dispatch_policy.name
      DedupeJob.perform_later(1)
      Tick.run(policy_name: policy_name)
      assert_equal 1, StagedJob.admitted.where(policy_name: policy_name).count

      DedupeJob.perform_later(1) # re-stage same key — partial unique index drops it

      assert_equal 1, StagedJob.where(policy_name: policy_name).count,
        "the partial unique index covers admitted-but-not-completed rows"
    end

    test "dedupe accepts a fresh stage after the previous row completes" do
      policy_name = DedupeJob.resolved_dispatch_policy.name
      DedupeJob.perform_later(1)
      Tick.run(policy_name: policy_name)
      staged = StagedJob.find_by!(policy_name: policy_name)

      # Simulate the around_perform completion writing completed_at.
      staged.update!(completed_at: Time.current)

      DedupeJob.perform_later(1)

      rows = StagedJob.where(policy_name: policy_name).order(:id)
      assert_equal 2, rows.count, "completed_at NOT NULL takes the row out of the partial unique index"
      assert_not_nil rows.first.completed_at
      assert_nil     rows.last.completed_at
    end

    # ───── Group C: reaper + completion race resilience ─────

    class TrackingJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy { gate :concurrency, max: 5, partition_by: ->(_ctx) { "rc" } }
      def perform(*); end
    end

    test "reap followed by release for the same row leaves the counter at 0 (clamp holds)" do
      policy_name = TrackingJob.resolved_dispatch_policy.name
      TrackingJob.perform_later
      Tick.run(policy_name: policy_name)
      assert_equal 1, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight)

      staged = StagedJob.admitted.find_by!(policy_name: policy_name)
      staged.update_columns(lease_expires_at: 1.minute.ago)

      Tick.reap
      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight),
        "reaper must release the counter for an expired lease"

      # Now around_perform completes the same job: Tick.release runs.
      # The clamp prevents the counter from going negative.
      Tick.release(policy_name: policy_name, partitions: { concurrency: "rc" })

      assert_equal 0, PartitionInflightCount.where(policy_name: policy_name).sum(:in_flight),
        "counter must stay non-negative when reap and release both fire for the same row"
    end

    class ThrottledOnlyJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :throttle, rate: 5, per: 60, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "reaper handles gates without tracks_inflight (e.g. throttle) without erroring" do
      policy_name = ThrottledOnlyJob.resolved_dispatch_policy.name
      ThrottledOnlyJob.perform_later("a")
      Tick.run(policy_name: policy_name)
      staged = StagedJob.admitted.find_by!(policy_name: policy_name)
      staged.update_columns(lease_expires_at: 1.minute.ago)

      assert_nothing_raised { Tick.reap }
      assert_not_nil staged.reload.completed_at
    end

    # ───── Group E: adaptive concurrency floor ─────

    class FlooredAdaptiveJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        gate :adaptive_concurrency,
             partition_by:  ->(_ctx) { "fp" },
             initial_max:   5,
             min:           2,
             target_lag_ms: 100
      end
      def perform(*); end
    end

    test "adaptive_concurrency current_max never drops below min across many slow observations" do
      policy_name = FlooredAdaptiveJob.resolved_dispatch_policy.name
      gate = FlooredAdaptiveJob.resolved_dispatch_policy.gates.first

      # Seed the row.
      AdaptiveConcurrencyStats.seed!(
        policy_name:   policy_name,
        gate_name:     :adaptive_concurrency,
        partition_key: "fp",
        initial_max:   5
      )

      # Twenty observations far over target — would shrink to 0 without the floor.
      20.times { gate.record_observation(partition_key: "fp", queue_lag_ms: 5_000, succeeded: true) }

      stats = AdaptiveConcurrencyStats.find_by!(policy_name: policy_name, partition_key: "fp")
      assert_equal 2, stats.current_max,
        "current_max must clamp at min=2 regardless of how many slow observations land"
    end

    test "adaptive_concurrency floor still applies under failure observations" do
      policy_name = FlooredAdaptiveJob.resolved_dispatch_policy.name
      gate = FlooredAdaptiveJob.resolved_dispatch_policy.gates.first

      AdaptiveConcurrencyStats.seed!(
        policy_name:   policy_name,
        gate_name:     :adaptive_concurrency,
        partition_key: "fp",
        initial_max:   5
      )

      10.times { gate.record_observation(partition_key: "fp", queue_lag_ms: 0, succeeded: false) }

      stats = AdaptiveConcurrencyStats.find_by!(policy_name: policy_name, partition_key: "fp")
      assert_equal 2, stats.current_max,
        "min=2 floor enforced even when failures would otherwise halve below"
    end
  end
end
