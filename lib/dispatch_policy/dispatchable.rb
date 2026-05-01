# frozen_string_literal: true

module DispatchPolicy
  module Dispatchable
    extend ActiveSupport::Concern

    class_methods do
      def dispatch_policy(&block)
        @dispatch_policy = DispatchPolicy::Policy.new(self, &block)
      end

      def dispatch_policy?
        !@dispatch_policy.nil?
      end

      def resolved_dispatch_policy
        @dispatch_policy
      end

      def inherited(subclass)
        super
        subclass.instance_variable_set(:@dispatch_policy, @dispatch_policy)
      end
    end

    included do
      attr_accessor :_dispatch_policy_name, :_dispatch_partition_key, :_dispatch_admitted_at

      around_perform do |job, block|
        succeeded = false
        begin
          block.call
          succeeded = true
        ensure
          policy_name   = job._dispatch_policy_name   || job.class.resolved_dispatch_policy&.name
          partition_key = job._dispatch_partition_key

          if policy_name && partition_key
            DispatchPolicy::Dispatch.release(
              policy_name: policy_name, partition_key: partition_key
            )
          end
          DispatchPolicy::StagedJob.mark_completed_by_active_job_id(job.job_id)
        end
        succeeded
      end
    end

    # Replaces ActiveJob's enqueue: route through staging unless we're
    # in the bypass path (the dispatch tx already set _dispatch_admitted_at).
    def enqueue(options = {})
      return super unless self.class.dispatch_policy?
      return super(options.except(:_bypass_staging)) if options[:_bypass_staging]
      return super unless DispatchPolicy.enabled?

      self.scheduled_at = options[:wait].seconds.from_now if options[:wait]
      self.scheduled_at = options[:wait_until]            if options[:wait_until]
      self.queue_name   = self.class.queue_name_from_part(options[:queue]) if options[:queue]
      self.priority     = options[:priority].to_i if options[:priority]

      DispatchPolicy::StagedJob.stage!(
        job_instance: self,
        policy:       self.class.resolved_dispatch_policy
      )
      self
    end

    def serialize
      super.merge(
        "_dispatch_policy_name"   => _dispatch_policy_name,
        "_dispatch_partition_key" => _dispatch_partition_key,
        "_dispatch_admitted_at"   => _dispatch_admitted_at&.iso8601(6)
      )
    end

    def deserialize(job_data)
      super
      self._dispatch_policy_name   = job_data["_dispatch_policy_name"]
      self._dispatch_partition_key = job_data["_dispatch_partition_key"]
      ts                           = job_data["_dispatch_admitted_at"]
      self._dispatch_admitted_at   = ts ? Time.iso8601(ts) : nil
    end
  end
end
