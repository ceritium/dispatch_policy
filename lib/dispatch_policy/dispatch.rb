# frozen_string_literal: true

module DispatchPolicy
  # Raised inside the dispatch transaction when the configured adapter
  # politely declines an enqueue (sets enqueue_error without raising).
  # Caught at the top of Dispatch.run to log and swallow after rollback.
  class EnqueueDeclined < StandardError; end

  # Cursor-based dispatcher.
  #
  #   1. Pluck the next `batch_size` partitions in last_checked_at
  #      ASC NULLS FIRST order — the most-stale-cursor first.
  #   2. For each, compute slot capacity from gate state. Partitions
  #      whose gates block them get slots = 0.
  #   3. CTE+LATERAL fetch into staged_jobs for partitions with slots
  #      > 0, bounded by batch_size.
  #   4. Mark admitted, hand to the adapter (same TX), and bump
  #      last_checked_at on EVERY visited partition (admitted or not)
  #      so the cursor rotates past them next tick.
  #
  # The "bump on every visit" is the key property: partitions
  # gate-blocked don't stop the cursor, they just lose a turn. With
  # N partitions and batch_size B, every partition is re-evaluated
  # every ceil(N/B) ticks regardless of state.
  class Dispatch
    def self.run(policy_name: nil)
      return 0 unless DispatchPolicy.enabled?

      ActiveSupport::Notifications.instrument(
        "tick.dispatch_policy",
        policy_name: policy_name
      ) do |payload|
        admitted          = 0
        partitions        = 0
        active_partitions = 0
        cursor_lag_ms     = nil

        begin
          StagedJob.transaction do
            policies(policy_name).each do |pname|
              policy = lookup_policy(pname)
              next unless policy

              cursor = PolicyPartition.pluck_cursor(pname, limit: policy.effective_batch_size)
              next if cursor.empty?

              # Cursor lag = age of the oldest visited partition's
              # last_checked_at. NULL means "never visited" → count
              # as max lag for the current tick. Take the max across
              # policies in the same tick.
              oldest = cursor.first["last_checked_at"]
              this_lag_ms =
                if oldest.nil?
                  0.0
                else
                  ((Time.current - oldest) * 1000.0).clamp(0, Float::INFINITY)
                end
              cursor_lag_ms = [ cursor_lag_ms || 0.0, this_lag_ms ].max

              # active_partitions = how many had pending demand at
              # the start of this iteration. Bounded by batch_size
              # in the cursor pluck; we count separately for accurate
              # reporting independent of batch_size.
              active_partitions += PolicyPartition
                .where(policy_name: pname)
                .where("pending_count > 0")
                .count

              partitions += cursor.size
              admitted   += dispatch_visited!(policy, cursor)
            end
          end
        rescue EnqueueDeclined => e
          Rails.logger&.warn("[DispatchPolicy] #{e.message} — transaction rolled back")
          payload[:declined]          = true
          payload[:admitted]          = 0
          payload[:partitions]        = partitions
          payload[:active_partitions] = active_partitions
          payload[:cursor_lag_ms]     = cursor_lag_ms
          return 0
        end

        payload[:admitted]          = admitted
        payload[:partitions]        = partitions
        payload[:active_partitions] = active_partitions
        payload[:cursor_lag_ms]     = cursor_lag_ms
        admitted
      end
    end

    # Lease recovery for workers that died mid-perform. Bulk: one
    # UPDATE on staged_jobs RETURNING (policy_name, partition_key)
    # feeds a single bulk_decrement_in_flight call per policy.
    def self.reap
      ActiveSupport::Notifications.instrument("reap.dispatch_policy") do |payload|
        now = Time.current

        complete_sql = <<~SQL.squish
          UPDATE #{StagedJob.quoted_table_name}
             SET completed_at     = ?,
                 lease_expires_at = NULL
           WHERE completed_at IS NULL
             AND lease_expires_at IS NOT NULL
             AND lease_expires_at < ?
          RETURNING policy_name, partition_key
        SQL

        expired = StagedJob.find_by_sql([ complete_sql, now, now ])
        if expired.empty?
          payload[:reaped] = 0
          next 0
        end

        per_policy = expired.group_by(&:policy_name)
        per_policy.each do |pname, rows|
          counts = rows.group_by(&:partition_key).transform_values(&:size)
          PolicyPartition.bulk_decrement_in_flight!(
            policy_name: pname, counts: counts, now: now
          )
        end

        payload[:reaped] = expired.size
        expired.size
      end
    end

    # Called from around_perform. Drops in_flight; the cursor will
    # pick this partition up on its next turn and admit anything new
    # the headroom allows.
    def self.release(policy_name:, partition_key:)
      PolicyPartition.release!(policy_name: policy_name, partition_key: partition_key)
    end

    def self.purge_drained_partitions(ttl_seconds: nil)
      ttl = ttl_seconds || DispatchPolicy.config.partition_idle_ttl
      return if ttl.nil? || ttl <= 0
      PolicyPartition.purge_drained!(ttl_seconds: ttl)
    end

    # ─── Internals ──────────────────────────────────────────────

    def self.policies(policy_name)
      return [ policy_name ] if policy_name

      PolicyPartition
        .where("pending_count > 0")
        .distinct
        .pluck(:policy_name)
    end

    def self.lookup_policy(policy_name)
      job_class = DispatchPolicy.registry[policy_name] || autoload_job_for(policy_name)
      job_class&.resolved_dispatch_policy
    end

    def self.autoload_job_for(policy_name)
      const_name = policy_name.tr("-", "/").camelize
      const_name.safe_constantize
      DispatchPolicy.registry[policy_name]
    end

    # Process all visited partitions: admit what fits, bump cursor.
    # Returns admitted count.
    def self.dispatch_visited!(policy, cursor)
      now        = Time.current
      batch_size = policy.effective_batch_size

      # Compute per-partition slots. Even gate-blocked ones go in
      # `visits` with delta=0 so their last_checked_at advances.
      visits     = {}
      admissible = []
      remaining  = batch_size
      cursor.each do |row|
        pk = row["partition_key"]
        visits[pk] = 0
        next if remaining <= 0
        slots = PolicyPartition.slots_for(row, batch_size: remaining, now: now)
        next if slots.zero?
        admissible << { partition_key: pk, slots: slots }
        remaining -= slots
      end

      batch = admissible.empty? ? [] : fetch_batch(policy, admissible)
      admit_count = batch.empty? ? 0 : admit_and_enqueue!(policy, batch, now: now, visits: visits)

      # Visit-only update: any partitions we plucked but didn't
      # admit from get last_checked_at bumped here. The admit path
      # already bumped its own subset via bulk_visit!.
      remaining_visits = visits.select { |_, d| d.zero? }
      PolicyPartition.bulk_visit!(
        policy_name: policy.name,
        visits:      remaining_visits,
        now:         now
      ) unless remaining_visits.empty?

      admit_count
    end

    # CTE + LATERAL: one query, one row per admissible partition's
    # slot quota. PG plans this as a parameterised values-driven scan.
    def self.fetch_batch(policy, admissible)
      values_clause = ([ "(?, ?::int)" ] * admissible.size).join(", ")
      values_args   = admissible.flat_map { |row| [ row[:partition_key], row[:slots] ] }

      sql = <<~SQL.squish
        WITH plan(partition_key, slots) AS (VALUES #{values_clause})
        SELECT rows.*
        FROM plan
        CROSS JOIN LATERAL (
          SELECT *
          FROM #{StagedJob.quoted_table_name}
          WHERE policy_name   = ?
            AND admitted_at   IS NULL
            AND partition_key = plan.partition_key
            AND (not_before_at IS NULL OR not_before_at <= NOW())
          ORDER BY priority, staged_at
          LIMIT plan.slots
          FOR UPDATE SKIP LOCKED
        ) AS rows
        LIMIT ?
      SQL

      StagedJob.find_by_sql([ sql, *values_args, policy.name, policy.effective_batch_size ])
    end

    def self.admit_and_enqueue!(policy, batch, now:, visits:)
      lease_expires = now + policy.effective_lease_duration

      admit_rows  = []
      pairs       = []

      batch.each do |staged|
        job = staged.instantiate_active_job
        job._dispatch_policy_name   = policy.name
        job._dispatch_partition_key = staged.partition_key
        job._dispatch_admitted_at   = now

        admit_rows << [ staged.id, job.job_id ]
        visits[staged.partition_key] = (visits[staged.partition_key] || 0) + 1
        pairs << [ staged, job ]
      end

      StagedJob.mark_admitted_many!(
        rows:             admit_rows,
        admitted_at:      now,
        lease_expires_at: lease_expires
      )
      # Visit only the admitted partitions here; the no-admit ones
      # are bumped separately so a single SQL handles each subset.
      admitted_visits = visits.select { |_, d| d.positive? }
      PolicyPartition.bulk_visit!(
        policy_name: policy.name,
        visits:      admitted_visits,
        now:         now
      )

      jobs = pairs.map(&:last)
      ActiveJob.perform_all_later(jobs)

      jobs.each do |job|
        next if job.successfully_enqueued?
        raise EnqueueDeclined,
          "adapter declined active_job_id=#{job.job_id}: " \
          "#{job.enqueue_error&.class}: #{job.enqueue_error&.message}"
      end

      jobs.size
    end
  end
end
