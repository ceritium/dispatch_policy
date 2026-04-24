# frozen_string_literal: true

module DispatchPolicy
  class StagedJob < ApplicationRecord
    self.table_name = "dispatch_policy_staged_jobs"

    scope :pending,   -> { where(admitted_at: nil, completed_at: nil) }
    scope :admitted,  -> { where.not(admitted_at: nil).where(completed_at: nil) }
    scope :completed, -> { where.not(completed_at: nil) }
    scope :active,    -> { where(completed_at: nil) }
    scope :expired_leases, -> {
      admitted.where("lease_expires_at IS NOT NULL AND lease_expires_at < ?", Time.current)
    }

    # Stages a job in the admission queue. Returns the created row, or nil if
    # the policy declares a dedupe_key and an active row already exists.
    def self.stage!(job_instance:, policy:)
      dedupe_key = policy.build_dedupe_key(job_instance.arguments)

      if dedupe_key && exists?(policy_name: policy.name, dedupe_key: dedupe_key, completed_at: nil)
        return nil
      end

      create!(
        job_class:       job_instance.class.name,
        policy_name:     policy.name,
        arguments:       job_instance.serialize,
        snapshot:        policy.build_snapshot(job_instance.arguments),
        context:         policy.context_builder.call(job_instance.arguments),
        priority:        job_instance.priority || 100,
        not_before_at:   job_instance.scheduled_at,
        staged_at:       Time.current,
        dedupe_key:      dedupe_key,
        round_robin_key: policy.build_round_robin_key(job_instance.arguments)
      )
    rescue ActiveRecord::RecordNotUnique
      nil
    end

    # Batch-insert variant of stage!.
    def self.stage_many!(policy:, jobs:)
      return 0 if jobs.empty?

      now = Time.current
      rows = jobs.map do |job_instance|
        {
          job_class:       job_instance.class.name,
          policy_name:     policy.name,
          arguments:       job_instance.serialize,
          snapshot:        policy.build_snapshot(job_instance.arguments),
          context:         policy.context_builder.call(job_instance.arguments),
          priority:        job_instance.priority || 100,
          not_before_at:   job_instance.scheduled_at,
          staged_at:       now,
          dedupe_key:      policy.build_dedupe_key(job_instance.arguments),
          round_robin_key: policy.build_round_robin_key(job_instance.arguments),
          partitions:      {},
          created_at:      now,
          updated_at:      now
        }
      end

      result = insert_all(rows, unique_by: :idx_dp_staged_dedupe_active)
      result.rows.size
    end

    def self.mark_completed_by_active_job_id(active_job_id)
      return 0 if active_job_id.blank?
      where(active_job_id: active_job_id, completed_at: nil)
        .update_all(completed_at: Time.current, lease_expires_at: nil)
    end

    def mark_admitted!(partitions:)
      now = Time.current
      job = instantiate_active_job
      job._dispatch_partitions  = partitions
      job._dispatch_admitted_at = now

      update!(
        admitted_at:      now,
        lease_expires_at: now + DispatchPolicy.config.lease_duration,
        active_job_id:    job.job_id,
        partitions:       partitions
      )

      job
    end

    def instantiate_active_job
      ActiveJob::Base.deserialize(arguments)
    end
  end
end
