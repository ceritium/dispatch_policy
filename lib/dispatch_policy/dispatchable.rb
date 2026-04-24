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

      # Walk up the ancestor chain so subclasses inherit the parent policy.
      def inherited(subclass)
        super
        subclass.instance_variable_set(:@dispatch_policy, @dispatch_policy)
      end
    end

    included do
      attr_accessor :_dispatch_partitions

      around_perform do |job, block|
        begin
          block.call
        ensure
          policy_name = job.class.resolved_dispatch_policy&.name
          if job._dispatch_partitions.present?
            DispatchPolicy::Tick.release(
              policy_name: policy_name,
              partitions:  job._dispatch_partitions
            )
          end
          DispatchPolicy::StagedJob.mark_completed_by_active_job_id(job.job_id)
        end
      end
    end

    def enqueue(options = {})
      return super unless self.class.dispatch_policy?
      if options[:_bypass_staging]
        return super(options.except(:_bypass_staging))
      end
      return super unless DispatchPolicy.enabled?

      # Mirror Active Job's scheduling option handling before staging.
      self.scheduled_at = options[:wait].seconds.from_now if options[:wait]
      self.scheduled_at = options[:wait_until] if options[:wait_until]
      self.queue_name   = self.class.queue_name_from_part(options[:queue]) if options[:queue]
      self.priority     = options[:priority].to_i if options[:priority]

      DispatchPolicy::StagedJob.stage!(
        job_instance: self,
        policy:       self.class.resolved_dispatch_policy
      )
      self
    end

    def serialize
      super.merge("_dispatch_partitions" => _dispatch_partitions || {})
    end

    def deserialize(job_data)
      super
      self._dispatch_partitions = job_data["_dispatch_partitions"]
    end
  end
end
