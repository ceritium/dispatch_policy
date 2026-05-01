# frozen_string_literal: true

module DispatchPolicy
  # Payload + per-partition order. Stages a job at perform_later
  # time, marks admitted at dispatch time, marks completed at
  # around_perform exit time.
  #
  # Atomicity: stage! INSERTs the row AND increments
  # PolicyPartition.pending_count in the same TX. mark_admitted_many!
  # UPDATEs admitted_at + lease_expires_at AND PolicyPartition counters
  # in the same TX (called from Dispatch.run inside a single
  # transaction that also enqueues the adapter rows).
  class StagedJob < ApplicationRecord
    self.table_name = "dispatch_policy_staged_jobs"

    scope :pending,   -> { where(admitted_at: nil, completed_at: nil) }
    scope :admitted,  -> { where.not(admitted_at: nil).where(completed_at: nil) }
    scope :completed, -> { where.not(completed_at: nil) }
    scope :active,    -> { where(completed_at: nil) }
    scope :expired_leases, -> {
      admitted.where("lease_expires_at IS NOT NULL AND lease_expires_at < ?", Time.current)
    }

    # ─── Stage ───────────────────────────────────────────────────

    def self.context_for(job_instance, policy)
      built = policy.context_builder.call(job_instance.arguments)
      return {} unless built.is_a?(Hash)
      {
        queue_name: job_instance.queue_name,
        priority:   job_instance.priority
      }.merge(built.symbolize_keys)
    end

    # Single-job stage. Returns the row, or nil if dedupe collision.
    def self.stage!(job_instance:, policy:)
      now           = Time.current
      dedupe_key    = policy.build_dedupe_key(job_instance.arguments)
      partition_key = policy.build_partition_key(job_instance.arguments)

      if dedupe_key && exists?(policy_name: policy.name, dedupe_key: dedupe_key, completed_at: nil)
        return nil
      end

      transaction do
        staged = create!(
          job_class:       job_instance.class.name,
          policy_name:     policy.name,
          partition_key:   partition_key,
          arguments:       job_instance.serialize,
          context:         context_for(job_instance, policy),
          priority:        job_instance.priority || 100,
          dedupe_key:      dedupe_key,
          not_before_at:   job_instance.scheduled_at,
          staged_at:       now
        )

        PolicyPartition.bulk_seed!(
          policy: policy,
          seeds: [ {
            partition_key:    partition_key,
            delta:            1,
            concurrency_max:  policy.resolve_concurrency_max(job_instance.arguments)
          } ],
          now: now
        )

        staged
      end
    end

    # Bulk stage. Called from ActiveJob.perform_all_later. Skips
    # dedupe collisions (relies on the unique partial index to ignore
    # duplicates via ON CONFLICT DO NOTHING).
    def self.stage_many!(policy:, jobs:)
      return 0 if jobs.empty?

      now = Time.current

      rows = jobs.map do |job|
        {
          policy_name:   policy.name,
          partition_key: policy.build_partition_key(job.arguments),
          job_class:     job.class.name,
          arguments:     job.serialize,
          context:       context_for(job, policy),
          priority:      job.priority || 100,
          dedupe_key:    policy.build_dedupe_key(job.arguments),
          not_before_at: job.scheduled_at,
          staged_at:     now,
          created_at:    now,
          updated_at:    now
        }
      end

      # First, resolve concurrency_max per partition_key. Each job
      # in the batch may belong to a different partition; we want
      # to evaluate the proc once per partition (not once per job).
      caps_by_partition = {}
      jobs.each do |job|
        pk = policy.build_partition_key(job.arguments)
        caps_by_partition[pk] ||= policy.resolve_concurrency_max(job.arguments)
      end

      transaction do
        # `RETURNING partition_key` so we count only rows that
        # actually inserted (dedupe ON CONFLICT skips duplicates).
        result = insert_all(
          rows,
          unique_by:   "idx_dp_staged_dedupe_active",
          returning:   %i[partition_key]
        )
        actual_counts = Hash.new(0)
        result.rows.each { |row| actual_counts[row.first] += 1 }

        seeds = actual_counts.map do |pk, delta|
          { partition_key: pk, delta: delta, concurrency_max: caps_by_partition[pk] }
        end
        PolicyPartition.bulk_seed!(policy: policy, seeds: seeds, now: now) unless seeds.empty?
        actual_counts.values.sum
      end
    end

    # Bulk admission marker. Called from Dispatch inside the dispatch
    # TX. `rows` is [[id, active_job_id], ...].
    def self.mark_admitted_many!(rows:, admitted_at:, lease_expires_at:)
      return 0 if rows.empty?

      values_clause = ([ "(?::bigint, ?)" ] * rows.size).join(", ")
      values_args   = rows.flat_map { |id, ajid| [ id, ajid.to_s ] }

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS s
           SET admitted_at      = ?,
               lease_expires_at = ?,
               active_job_id    = d.active_job_id,
               updated_at       = ?
          FROM (VALUES #{values_clause}) AS d(id, active_job_id)
         WHERE s.id = d.id
      SQL

      connection.exec_update(
        sanitize_sql_array([ sql, admitted_at, lease_expires_at, admitted_at, *values_args ])
      )
      connection.clear_query_cache
    end

    def self.mark_completed_by_active_job_id(active_job_id, now: Time.current)
      return 0 unless active_job_id

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name}
           SET completed_at     = ?,
               lease_expires_at = NULL,
               updated_at       = ?
         WHERE active_job_id = ?
           AND completed_at IS NULL
      SQL
      connection.exec_update(sanitize_sql_array([ sql, now, now, active_job_id ]))
      connection.clear_query_cache
    end

    # Re-instantiate the ActiveJob from the stored serialized payload.
    def instantiate_active_job
      ActiveJob::Base.deserialize(arguments)
    end
  end
end
