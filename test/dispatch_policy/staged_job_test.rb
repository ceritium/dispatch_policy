# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class StagedJobTest < ActiveSupport::TestCase
    class BatchJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        dedupe_key ->(args) { "batch:#{args.first}" }
        round_robin_by ->(args) { args.first }
      end
      def perform(*); end
    end

    test "stage_many! batch-inserts and respects dedupe" do
      jobs = %w[a b c a].map { |k| BatchJob.new(k) }
      policy = BatchJob.resolved_dispatch_policy

      count = StagedJob.stage_many!(policy: policy, jobs: jobs)
      assert_equal 3, count # duplicate 'a' dropped
    end

    test "stage_many! populates context, dedupe_key, and round_robin_key" do
      StagedJob.stage_many!(policy: BatchJob.resolved_dispatch_policy, jobs: [ BatchJob.new("x") ])
      staged = StagedJob.pending.last
      assert_equal "x", staged.context["tenant"]
      assert_equal "batch:x", staged.dedupe_key
      assert_equal "x", staged.round_robin_key
    end

    test "stage_many! is a no-op for an empty array" do
      assert_equal 0, StagedJob.stage_many!(policy: BatchJob.resolved_dispatch_policy, jobs: [])
    end

    test "context auto-injects queue_name and priority alongside user keys" do
      BatchJob.set(queue: "urgent", priority: 3).perform_later("q1")
      ctx = StagedJob.pending.last.context
      assert_equal "urgent", ctx["queue_name"]
      assert_equal 3, ctx["priority"]
      assert_equal "q1", ctx["tenant"]
    end

    test "user context wins over auto-injected queue_name" do
      Class.new(ActiveJob::Base) do
        def self.name = "OverrideQueueJob"
        include DispatchPolicy::Dispatchable
        dispatch_policy do
          context ->(args) { { queue_name: "forced" } }
        end
        def perform(*); end
      end.set(queue: "ignored").perform_later("x")

      assert_equal "forced", StagedJob.pending.last.context["queue_name"]
    end

    test "mark_completed_by_active_job_id updates only the matching row" do
      BatchJob.perform_later("z")
      Tick.run(policy_name: BatchJob.resolved_dispatch_policy.name)
      staged = StagedJob.admitted.last
      assert_equal 1, StagedJob.mark_completed_by_active_job_id(staged.active_job_id)
      assert_not_nil staged.reload.completed_at
    end
  end
end
