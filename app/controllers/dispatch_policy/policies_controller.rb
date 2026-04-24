# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    STALE_PENDING_THRESHOLD = 1.hour

    before_action :load_policy, only: :show

    def index
      @policies = DispatchPolicy.registry.map do |name, job_class|
        scope   = StagedJob.where(policy_name: name)
        pending = scope.pending
        {
          name:            name,
          job_class:       job_class,
          policy:          job_class.resolved_dispatch_policy,
          pending_count:   pending.count,
          admitted_count:  scope.admitted.count,
          completed_24h:   scope.completed.where(completed_at: 24.hours.ago..).count,
          oldest_pending:  pending.minimum(:staged_at),
          stale_threshold: STALE_PENDING_THRESHOLD
        }
      end.sort_by { |p| -p[:pending_count] }

      @active_partitions = PartitionInflightCount.where("in_flight > 0").count
      @expired_leases    = StagedJob.expired_leases.count
    end

    def show
      scope = StagedJob.where(policy_name: @policy_name)
      @pending_count           = scope.pending.count
      @pending_eligible_count  = scope.pending.where("not_before_at IS NULL OR not_before_at <= ?", Time.current).count
      @pending_scheduled_count = @pending_count - @pending_eligible_count
      @admitted_count          = scope.admitted.count
      @completed_24h           = scope.completed.where(completed_at: 24.hours.ago..).count

      @partition_counts = PartitionInflightCount
        .where(policy_name: @policy_name).order(updated_at: :desc).limit(50)
      @throttle_buckets = ThrottleBucket
        .where(policy_name: @policy_name).order(:gate_name, :partition_key).limit(50)
      @pending_jobs = scope.pending.order(:priority, :staged_at).limit(50)
    end

    private

    def load_policy
      @policy_name = params[:policy_name]
      @job_class   = DispatchPolicy.registry[@policy_name]
      raise ActiveRecord::RecordNotFound unless @job_class
      @policy = @job_class.resolved_dispatch_policy
    end
  end
end
