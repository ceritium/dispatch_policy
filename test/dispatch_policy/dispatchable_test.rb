# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class DispatchableTest < ActiveSupport::TestCase
    class DedupeJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        dedupe_key ->(args) { "dedupe:#{args.first}" }
      end
      def perform(*); end
    end

    class NoPolicyJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      def perform(*); end
    end

    test "perform_later stages a job when dispatch_policy is declared" do
      assert_difference -> { StagedJob.pending.count }, +1 do
        DedupeJob.perform_later("A")
      end
    end

    test "dedupe_key prevents a second stage while the first is pending" do
      DedupeJob.perform_later("A")
      assert_no_difference -> { StagedJob.count } do
        DedupeJob.perform_later("A")
      end
    end

    test "jobs without a dispatch_policy bypass staging" do
      assert_no_difference -> { StagedJob.count } do
        NoPolicyJob.perform_later("A")
      end
    end

    test "_bypass_staging option skips the staging path" do
      assert_no_difference -> { StagedJob.count } do
        DedupeJob.new("A").enqueue(_bypass_staging: true)
      end
    end

    test "disabled config bypasses staging" do
      DispatchPolicy.config.enabled = false
      begin
        assert_no_difference -> { StagedJob.count } do
          DedupeJob.perform_later("A")
        end
      ensure
        DispatchPolicy.config.enabled = true
      end
    end

    test "around_perform releases counters and marks the staged row completed" do
      DedupeJob.perform_later("Z")
      Tick.run(policy_name: DedupeJob.resolved_dispatch_policy.name)

      staged = StagedJob.where(policy_name: DedupeJob.resolved_dispatch_policy.name).order(:id).last
      assert_not_nil staged.active_job_id

      # Simulate the worker running the perform — re-instantiate from the stored args.
      job = ActiveJob::Base.deserialize(staged.arguments)
      job._dispatch_partitions = staged.partitions
      job.perform_now

      assert_not_nil staged.reload.completed_at
    end
  end
end
