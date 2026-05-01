# frozen_string_literal: true

module DispatchPolicy
  # Around-perform that records each job execution in
  # dispatch_policy_inflight_jobs while it runs, so the concurrency gate
  # can count active jobs per partition.
  module InflightTracker
    extend ActiveSupport::Concern

    class_methods do
      def dispatch_policy_inflight_tracking
        around_perform do |job, block|
          DispatchPolicy::InflightTracker.track(job, &block)
        end
      end
    end

    def self.track(job)
      policy_name = job.class.respond_to?(:dispatch_policy_name) && job.class.dispatch_policy_name
      unless policy_name
        return yield
      end

      policy = DispatchPolicy.registry.fetch(policy_name)
      unless policy
        return yield
      end

      ctx              = policy.build_context(job.arguments)
      concurrency_gate = policy.gates.find { |g| g.name == :concurrency }

      if concurrency_gate
        Repository.insert_inflight!([{
          policy_name:    policy.name,
          partition_key:  concurrency_gate.inflight_partition_key(policy.name, ctx),
          active_job_id:  job.job_id
        }])
      end

      begin
        yield
      ensure
        Repository.delete_inflight!(active_job_id: job.job_id) if concurrency_gate
      end
    end
  end
end
