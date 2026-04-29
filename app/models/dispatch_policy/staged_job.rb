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

    # Merge the job's ActiveJob metadata (queue_name, priority) into the
    # context hash so gate lambdas can partition_by :queue_name without
    # the user having to pass it as a kwarg. User-provided keys win.
    def self.context_for(job_instance, policy)
      built = policy.context_builder.call(job_instance.arguments)
      return built unless built.is_a?(Hash)
      {
        queue_name: job_instance.queue_name,
        priority:   job_instance.priority
      }.merge(built.symbolize_keys)
    end

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
        context:         context_for(job_instance, policy),
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
          context:         context_for(job_instance, policy),
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

    # Bulk variant of mark_admitted! for the tick path. Updates every
    # row in the batch with one UPDATE … FROM (VALUES …) statement
    # instead of N separate UPDATEs. The (id, active_job_id,
    # partitions) tuples are computed in Ruby — the tick path
    # deserializes the ActiveJob and assigns _dispatch_partitions in
    # memory there, so we don't need a RETURNING clause.
    def self.mark_admitted_many!(rows:, admitted_at:, lease_expires_at:)
      return 0 if rows.empty?

      values_clause = ([ "(?::bigint, ?, ?::jsonb)" ] * rows.size).join(", ")
      values_args   = rows.flat_map { |id, active_job_id, partitions|
        [ id, active_job_id, partitions.to_json ]
      }

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS s
           SET admitted_at      = ?,
               lease_expires_at = ?,
               active_job_id    = d.active_job_id,
               partitions       = d.partitions
          FROM (VALUES #{values_clause}) AS d(id, active_job_id, partitions)
         WHERE s.id = d.id
      SQL

      connection.exec_update(
        sanitize_sql_array([ sql, admitted_at, lease_expires_at, *values_args ])
      )
      # exec_update with raw SQL doesn't invalidate the AR query
      # cache the way update_all does. Without this, callers running
      # subsequent SELECTs against staged_jobs in the same query-cache
      # scope see stale rows. TickLoop wraps in uncached so this is a
      # no-op there, but we want correct semantics on direct callers.
      connection.clear_query_cache
    end

    def instantiate_active_job
      ActiveJob::Base.deserialize(arguments)
    end
  end
end
