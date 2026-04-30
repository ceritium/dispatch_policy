# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class PolicyTest < ActiveSupport::TestCase
    class SampleJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        dedupe_key ->(args) { "sample:#{args.first}" }
        round_robin_by ->(args) { args.first }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "registers the policy under the underscored class name" do
      policy = SampleJob.resolved_dispatch_policy
      assert_equal "dispatch_policy-policy_test-sample_job", policy.name
      assert_equal SampleJob, DispatchPolicy.registry[policy.name]
    end

    test "context, dedupe_key, and round_robin_by all invoke their lambdas" do
      policy = SampleJob.resolved_dispatch_policy
      assert_equal({ tenant: "A" }, policy.context_builder.call([ "A" ]))
      assert_equal "sample:A", policy.build_dedupe_key([ "A" ])
      assert_equal "A", policy.build_round_robin_key([ "A" ])
    end

    test "round_robin_key returns nil for empty/nil values" do
      policy = SampleJob.resolved_dispatch_policy
      assert_nil policy.build_round_robin_key([ nil ])
      assert_nil policy.build_round_robin_key([ "" ])
    end

    # ─── Per-policy config overrides ───────────────────────────────

    class TunedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        round_robin_by      ->(args) { args.first }
        batch_size          1000
        round_robin_quantum 1
        lease_duration      5 * 60
        round_robin_max_partitions_per_tick 250
      end
      def perform(*); end
    end

    test "per-policy DSL overrides win over DispatchPolicy.config defaults" do
      policy = TunedJob.resolved_dispatch_policy

      assert_equal 1000,    policy.effective_batch_size
      assert_equal 1,       policy.effective_round_robin_quantum
      assert_equal 5 * 60,  policy.effective_lease_duration
      assert_equal 250,     policy.effective_round_robin_max_partitions_per_tick
    end

    test "policy without overrides falls back to DispatchPolicy.config" do
      policy = SampleJob.resolved_dispatch_policy

      assert_equal DispatchPolicy.config.batch_size,          policy.effective_batch_size
      assert_equal DispatchPolicy.config.round_robin_quantum, policy.effective_round_robin_quantum
      assert_equal DispatchPolicy.config.lease_duration,      policy.effective_lease_duration
    end

    test "config_overrides reports only the overridden knobs" do
      tuned     = TunedJob.resolved_dispatch_policy
      untuned   = SampleJob.resolved_dispatch_policy

      assert_equal 1000, tuned.config_overrides[:batch_size]
      assert_equal 1,    tuned.config_overrides[:round_robin_quantum]
      assert_empty       untuned.config_overrides
    end

    test "DSL setters can also be invoked at runtime to update config" do
      policy = SampleJob.resolved_dispatch_policy
      original_batch = policy.config_overrides[:batch_size]

      policy.batch_size 200
      assert_equal 200, policy.effective_batch_size
      assert_equal 200, policy.config_overrides[:batch_size]
    ensure
      # Restore for other tests in random order
      policy.instance_variable_set(:@override_batch_size, original_batch)
    end
  end
end
