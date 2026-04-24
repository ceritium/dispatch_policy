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
  end
end
