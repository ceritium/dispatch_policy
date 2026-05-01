# frozen_string_literal: true

module DispatchPolicy
  # Raised inside the dispatch transaction when the configured adapter
  # politely declines an enqueue (sets enqueue_error without raising).
  # Caught at the top of Dispatch.run to log and swallow after rollback.
  class EnqueueDeclined < StandardError; end

  # Single admission pass:
  #
  #   1. unblock_due!(policy)         — flip ready=TRUE on partitions whose
  #                                     blocked_until has passed.
  #   2. PolicyPartition.pluck_admissible — pick partitions in LRU order
  #                                         with the slot count each can take.
  #   3. CTE+LATERAL fetch over staged_jobs, one shot, bounded.
  #   4. PolicyPartition.bulk_admit!  — decrement pending, increment in_flight,
  #                                     drain throttle tokens, recompute ready.
  #   5. ActiveJob.perform_all_later  — adapter handoff in same TX.
  #
  # Anything fails → rollback unwinds admission, demand counters and
  # adapter rows together. The dispatcher never fetches a job whose
  # partition isn't admissible, so there's no filter step and no work
  # wasted on rejected admissions.
  class Dispatch
    def self.run(policy_name: nil)
      return 0 unless DispatchPolicy.enabled?

      ActiveSupport::Notifications.instrument(
        "tick.dispatch_policy",
        policy_name: policy_name
      ) do |payload|
        admitted   = 0
        partitions = 0

        begin
          StagedJob.transaction do
            policies(policy_name).each do |pname|
              policy = lookup_policy(pname)
              next unless policy

              PolicyPartition.unblock_due!(policy_name: pname)

              admissible = PolicyPartition.pluck_admissible(
                pname,
                limit:      policy.effective_batch_size,
                batch_size: policy.effective_batch_size
              )
              next if admissible.empty?

              batch = fetch_batch(policy, admissible)
              next if batch.empty?

              partitions += admissible.size
              admitted   += admit_and_enqueue!(policy, batch)
            end
          end
        rescue EnqueueDeclined => e
          Rails.logger&.warn("[DispatchPolicy] #{e.message} — transaction rolled back")
          payload[:declined]   = true
          payload[:admitted]   = 0
          payload[:partitions] = partitions
          return 0
        end

        payload[:admitted]   = admitted
        payload[:partitions] = partitions
        admitted
      end
    end

    # Lease recovery for workers that died mid-perform without
    # releasing their in_flight counter. Bulk: one UPDATE on
    # staged_jobs RETURNING (policy_name, partition_key) feeds a
    # single bulk_decrement_in_flight call.
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

    # Called from around_perform after a job completes (or fails).
    # Decrements in_flight + recomputes ready for the partition.
    def self.release(policy_name:, partition_key:)
      PolicyPartition.release!(policy_name: policy_name, partition_key: partition_key)
    end

    # Periodic-ish maintenance. Called from TickLoop boot.
    def self.purge_drained_partitions(ttl_seconds: nil)
      ttl = ttl_seconds || DispatchPolicy.config.partition_idle_ttl
      return if ttl.nil? || ttl <= 0
      PolicyPartition.purge_drained!(ttl_seconds: ttl)
    end

    # ─── Internals ──────────────────────────────────────────────

    def self.policies(policy_name)
      return [ policy_name ] if policy_name

      PolicyPartition
        .where("pending_count > 0 AND ready = TRUE")
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

    # CTE + LATERAL: one query, one row per admissible partition's
    # slot quota. PG plans this as a parameterised values-driven scan;
    # complexity is O(admissible_count × log(staged_jobs)) per the
    # idx_dp_staged_partition_order partial index.
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

    def self.admit_and_enqueue!(policy, batch)
      now           = Time.current
      lease_expires = now + policy.effective_lease_duration

      admit_rows  = []
      counts      = Hash.new(0)
      pairs       = []

      batch.each do |staged|
        job = staged.instantiate_active_job
        job._dispatch_policy_name   = policy.name
        job._dispatch_partition_key = staged.partition_key
        job._dispatch_admitted_at   = now

        admit_rows << [ staged.id, job.job_id ]
        counts[staged.partition_key] += 1
        pairs << [ staged, job ]
      end

      StagedJob.mark_admitted_many!(
        rows:             admit_rows,
        admitted_at:      now,
        lease_expires_at: lease_expires
      )
      PolicyPartition.bulk_admit!(
        policy_name: policy.name,
        counts:      counts,
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
