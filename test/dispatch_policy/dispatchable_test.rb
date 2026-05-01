# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class DispatchableTest < ActiveSupport::TestCase
    class WorkerJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        concurrency  max: 1
      end
      def perform(*); end
    end

    test "around_perform releases the partition's in_flight" do
      WorkerJob.perform_later("a")
      Dispatch.run(policy_name: WorkerJob.resolved_dispatch_policy.name)

      staged = StagedJob.admitted.last
      part   = PolicyPartition.find_by(partition_key: "a")
      assert_equal 1, part.in_flight

      job = ActiveJob::Base.deserialize(staged.arguments)
      job._dispatch_policy_name   = staged.policy_name
      job._dispatch_partition_key = staged.partition_key
      job.perform_now

      assert_equal 0, part.reload.in_flight
      assert_not_nil staged.reload.completed_at
    end

    test "around_perform releases in_flight so the cursor admits next time" do
      # max=1, two pending. First admit → cap. perform → release → next tick admits.
      2.times { WorkerJob.perform_later("a") }
      Dispatch.run(policy_name: WorkerJob.resolved_dispatch_policy.name)
      part = PolicyPartition.find_by(partition_key: "a")
      assert_equal 1, part.in_flight
      assert_equal 1, part.pending_count

      staged = StagedJob.admitted.last
      job = ActiveJob::Base.deserialize(staged.arguments)
      job._dispatch_policy_name   = staged.policy_name
      job._dispatch_partition_key = staged.partition_key
      job.perform_now

      part.reload
      assert_equal 0, part.in_flight
      assert_equal 1, part.pending_count

      Dispatch.run(policy_name: WorkerJob.resolved_dispatch_policy.name)
      assert_equal 1, part.reload.in_flight, "next tick should re-admit now that there's headroom"
    end
  end
end
