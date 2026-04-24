# frozen_string_literal: true

module DispatchPolicy
  # Rails 7.1's ActiveJob.perform_all_later(*jobs) bypasses ActiveJob::Base#enqueue
  # and calls queue_adapter.enqueue_all directly. Dispatchable hooks on #enqueue,
  # so without this patch the batch path would skip staging.
  module ActiveJobPerformAllLaterPatch
    def perform_all_later(*jobs)
      jobs.flatten!

      staged, remaining = jobs.partition do |job|
        klass = job.class
        klass.respond_to?(:dispatch_policy?) &&
          klass.dispatch_policy? &&
          DispatchPolicy.enabled?
      end

      staged_count = 0
      staged.group_by(&:class).each do |klass, group|
        staged_count += DispatchPolicy::StagedJob.stage_many!(
          policy: klass.resolved_dispatch_policy,
          jobs:   group
        )
      end

      remaining_count = remaining.empty? ? 0 : super(*remaining)
      staged_count + remaining_count.to_i
    end
  end
end

ActiveJob.singleton_class.prepend(DispatchPolicy::ActiveJobPerformAllLaterPatch)
