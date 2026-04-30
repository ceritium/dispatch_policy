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
      attr_accessor :_dispatch_partitions, :_dispatch_admitted_at

      around_perform do |job, block|
        # queue_lag = admitted_at → perform_start. Pure signal for "is the
        # adapter queue building up?" (high = admitting too fast) vs "are
        # workers idle?" (near zero = ready for more). Measured BEFORE
        # block.call so perform duration doesn't pollute it.
        admitted_at   = job._dispatch_admitted_at
        perform_start = Time.current
        queue_lag_ms  = admitted_at ? ((perform_start - admitted_at) * 1000).to_i : 0

        succeeded = false
        begin
          block.call
          succeeded = true
        ensure
          duration_ms = ((Time.current - perform_start) * 1000).to_i
          policy_name = job.class.resolved_dispatch_policy&.name

          policy = job.class.resolved_dispatch_policy
          if job._dispatch_partitions.present?
            DispatchPolicy::Tick.release(
              policy_name: policy_name,
              partitions:  job._dispatch_partitions
            )

            # Let adaptive gates update their AIMD state first; the
            # generic observation below then captures the resulting
            # current_max alongside lag + duration for the chart.
            job._dispatch_partitions.each do |gate_name, partition_key|
              gate = policy&.gates&.find { |g| g.name == gate_name.to_sym }
              next unless gate.is_a?(DispatchPolicy::Gates::AdaptiveConcurrency)
              gate.record_observation(
                partition_key: partition_key,
                queue_lag_ms:  queue_lag_ms,
                succeeded:     succeeded
              )
            end

            # Generic observation per unique partition. Every gate with
            # partition_by (adaptive or not) gets a sparkline this way,
            # plus :fair_time_share reads consumed_ms from here.
            job._dispatch_partitions.values.uniq.each do |partition_key|
              current_max = DispatchPolicy::AdaptiveConcurrencyStats.current_max_for(
                policy_name:   policy_name,
                partition_key: partition_key
              )
              DispatchPolicy::PartitionObservation.observe!(
                policy_name:   policy_name,
                partition_key: partition_key,
                queue_lag_ms:  queue_lag_ms,
                duration_ms:   duration_ms,
                current_max:   current_max
              )
            end
          end

          # Time-weighted round-robin reads PartitionObservation by
          # round_robin_key. If no gate's partition_by produces that
          # key (or the policy has no gates at all), the loop above
          # never recorded one and weight: :time would silently fall
          # back to equal round-robin. Backfill an observation here
          # so the time-share allocator has data without forcing the
          # operator to declare a "phantom" gate just to feed it.
          if policy&.round_robin? && policy.round_robin_weight == :time
            rr_key = policy.build_round_robin_key(job.arguments)
            already_recorded = (job._dispatch_partitions || {}).values.include?(rr_key)
            if rr_key && !already_recorded
              DispatchPolicy::PartitionObservation.observe!(
                policy_name:   policy_name,
                partition_key: rr_key,
                queue_lag_ms:  queue_lag_ms,
                duration_ms:   duration_ms,
                current_max:   nil
              )
            end
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
      super.merge(
        "_dispatch_partitions"  => _dispatch_partitions || {},
        "_dispatch_admitted_at" => _dispatch_admitted_at&.iso8601(6)
      )
    end

    def deserialize(job_data)
      super
      self._dispatch_partitions  = job_data["_dispatch_partitions"]
      ts                         = job_data["_dispatch_admitted_at"]
      self._dispatch_admitted_at = ts ? Time.iso8601(ts) : nil
    end
  end
end
