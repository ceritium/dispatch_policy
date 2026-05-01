# frozen_string_literal: true

module DispatchPolicy
  # Hooks into ActiveJob::Base. Adds:
  #   - the `dispatch_policy :name do … end` class macro
  #   - an `around_enqueue` callback that stages jobs declaring a policy
  #   - a `perform_all_later` patch that handles bulk enqueue
  module JobExtension
    extend ActiveSupport::Concern

    included do
      class_attribute :dispatch_policy_name, instance_writer: false
    end

    class_methods do
      def dispatch_policy(name, &block)
        policy = PolicyDSL.build(name.to_s, &block)
        DispatchPolicy.registry.register(policy, owner: self.name)
        self.dispatch_policy_name = policy.name

        around_enqueue do |job, block|
          DispatchPolicy::JobExtension.around_enqueue_for(job, block)
        end
      end
    end

    # Called by the around_enqueue lambda. Public so it can be tested directly.
    def self.around_enqueue_for(job, block)
      return block.call if Bypass.active?

      policy = DispatchPolicy.registry.fetch(job.class.dispatch_policy_name)
      return block.call unless policy

      if retry_attempt?(job) && policy.bypass_retries?
        return block.call
      end

      ctx           = policy.build_context(job.arguments)
      partition_key = policy.partition_key_for(ctx)
      payload       = Serializer.serialize(job)
      queue_name    = job.queue_name&.to_s || policy.queue_name

      Repository.stage!(
        policy_name:   policy.name,
        partition_key: partition_key,
        queue_name:    queue_name,
        job_class:     job.class.name,
        job_data:      payload,
        context:       ctx.to_jsonb,
        scheduled_at:  scheduled_time(job),
        priority:      job.priority || 0
      )

      job.successfully_enqueued = true
      false # halts the around_enqueue chain so the real adapter never sees the job
    end

    def self.retry_attempt?(job)
      (job.respond_to?(:executions) ? job.executions.to_i : 0).positive?
    end

    def self.scheduled_time(job)
      ts = job.scheduled_at
      return nil if ts.nil?
      return ts   if ts.is_a?(Time)

      Time.at(Float(ts))
    rescue ArgumentError, TypeError
      nil
    end

    # ---- perform_all_later support -------------------------------------------

    # Rails 7.1+ exposes ActiveJob.perform_all_later. We override it to route
    # jobs declaring a dispatch_policy through a single bulk INSERT.
    module BulkEnqueue
      def perform_all_later(*jobs, options: {})
        flat = jobs.flatten
        return super if flat.empty?
        return super unless DispatchPolicy.registry.size.positive?

        with_policy, without_policy = flat.partition { |j| j.class.respond_to?(:dispatch_policy_name) && j.class.dispatch_policy_name }
        super(without_policy, options: options) if without_policy.any?

        return if with_policy.empty?

        rows = with_policy.filter_map do |job|
          policy = DispatchPolicy.registry.fetch(job.class.dispatch_policy_name)
          next unless policy

          ctx           = policy.build_context(job.arguments)
          partition_key = policy.partition_key_for(ctx)
          payload       = Serializer.serialize(job)
          job.successfully_enqueued = true

          {
            policy_name:   policy.name,
            partition_key: partition_key,
            queue_name:    job.queue_name&.to_s || policy.queue_name,
            job_class:     job.class.name,
            job_data:      payload,
            context:       ctx.to_jsonb,
            scheduled_at:  JobExtension.scheduled_time(job),
            priority:      job.priority || 0
          }
        end

        Repository.stage_many!(rows)
      end
    end
  end
end
