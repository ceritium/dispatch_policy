# frozen_string_literal: true

module DispatchPolicy
  # Raised inside the staging transaction when the configured adapter
  # politely declines an enqueue (sets enqueue_error without raising).
  # Caught at the top of Tick.run to log and swallow after the rollback
  # has already reverted admission.
  class EnqueueDeclined < StandardError; end

  class Tick
    THROTTLE_ZERO_THRESHOLD = 0.001

    # Single admission pass: fetch pending staged jobs, run gates, mark
    # survivors as admitted, AND hand them off to the real adapter — all
    # inside one PostgreSQL transaction. Because dispatch_policy only
    # supports PG-backed adapters (GoodJob, Solid Queue) sharing the
    # same connection as our staging tables, the adapter's INSERT into
    # its own jobs table participates in the same TX. Any failure
    # (gate raise, adapter raise, polite decline) rolls back everything:
    # rows stay pending, counters never drift, no half-enqueued state.
    def self.run(policy_name: nil)
      return 0 unless DispatchPolicy.enabled?

      admitted = 0

      begin
        StagedJob.transaction do
          active_policies(policy_name).each do |pname|
            policy = lookup_policy(pname)
            next unless policy

            batch = fetch_batch(policy)
            next if batch.empty?

            run_policy(policy, batch).each do |staged, job|
              job.enqueue(_bypass_staging: true)

              # ActiveJob adapters can report failure by setting
              # enqueue_error and leaving successfully_enqueued? false
              # instead of raising. We treat that as a hard failure and
              # roll back the entire transaction so admission, counters
              # and the adapter row all unwind together.
              unless job.successfully_enqueued?
                raise EnqueueDeclined,
                  "adapter declined staged=#{staged.id}: " \
                  "#{job.enqueue_error&.class}: #{job.enqueue_error&.message}"
              end

              admitted += 1
            end
          end
        end
      rescue EnqueueDeclined => e
        Rails.logger&.warn("[DispatchPolicy] #{e.message} — transaction rolled back")
        return 0
      end

      admitted
    end

    def self.prune_idle_partitions
      ttl = DispatchPolicy.config.partition_idle_ttl
      return if ttl.nil? || ttl <= 0

      cutoff = Time.current - ttl
      PartitionInflightCount.where(in_flight: 0).where("updated_at < ?", cutoff).delete_all
      ThrottleBucket.where("tokens <= ? AND refilled_at < ?", THROTTLE_ZERO_THRESHOLD, cutoff).delete_all
    end

    def self.prune_orphan_gate_rows
      [ PartitionInflightCount, ThrottleBucket ].each do |model|
        model.distinct.pluck(:policy_name, :gate_name).each do |policy_name, gate_name|
          policy = lookup_policy(policy_name)
          next if policy && policy.gates.any? { |g| g.name == gate_name.to_sym }

          model.where(policy_name: policy_name, gate_name: gate_name).delete_all
        end
      end
    end

    # Safety net for the narrow case where a worker started executing
    # an admitted job but died before around_perform's ensure block
    # released its in-flight counters. With atomic admission+enqueue
    # there is no longer a "stuck admitted but never reached the
    # adapter" case to recover from — the adapter's own retry semantics
    # cover crashed jobs, dispatch_policy only releases the counters.
    #
    # Implementation is bulk: one UPDATE … RETURNING completes every
    # expired row and hands back their partitions, then a single
    # UPDATE … FROM (VALUES …) decrements all the (policy, gate,
    # partition_key) counters at once. This keeps reap O(1) in SQL
    # round-trips regardless of how many leases expired.
    def self.reap
      now = Time.current

      complete_sql = <<~SQL.squish
        UPDATE #{StagedJob.quoted_table_name}
           SET completed_at = ?,
               lease_expires_at = NULL
         WHERE completed_at IS NULL
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at < ?
        RETURNING policy_name, partitions
      SQL

      expired = StagedJob.find_by_sql([ complete_sql, now, now ])
      return 0 if expired.empty?

      # Aggregate per-(policy, gate, partition_key) decrements, filtered
      # to gates that actually track in-flight (throttle/etc. do not).
      deltas = Hash.new(0)
      expired.each do |row|
        policy = lookup_policy(row.policy_name)
        next unless policy

        (row.partitions || {}).each do |gate_name, partition_key|
          gate = policy.gates.find { |g| g.name == gate_name.to_sym }
          next unless gate&.tracks_inflight?

          deltas[[ row.policy_name, gate_name.to_s, partition_key.to_s ]] += 1
        end
      end

      return expired.size if deltas.empty?

      values_clause = ([ "(?, ?, ?, ?::int)" ] * deltas.size).join(", ")
      values_args   = deltas.flat_map { |(policy_name, gate, key), delta| [ policy_name, gate, key, delta ] }

      decrement_sql = <<~SQL.squish
        UPDATE #{PartitionInflightCount.quoted_table_name} AS c
           SET in_flight  = GREATEST(c.in_flight - d.delta, 0),
               updated_at = ?
          FROM (VALUES #{values_clause}) AS d(policy_name, gate_name, partition_key, delta)
         WHERE c.policy_name   = d.policy_name
           AND c.gate_name     = d.gate_name
           AND c.partition_key = d.partition_key
      SQL

      PartitionInflightCount.connection.exec_update(
        PartitionInflightCount.send(:sanitize_sql_array, [ decrement_sql, now, *values_args ])
      )

      expired.size
    end

    def self.release(policy_name:, partitions:)
      partitions.each do |gate_name, partition_key|
        policy = lookup_policy(policy_name)
        gate   = policy&.gates&.find { |g| g.name == gate_name.to_sym }
        next unless gate&.tracks_inflight?

        PartitionInflightCount.decrement(
          policy_name:   policy_name,
          gate_name:     gate_name.to_s,
          partition_key: partition_key.to_s
        )
      end
    end

    def self.active_policies(policy_name)
      return [ policy_name ] if policy_name

      StagedJob.pending
        .where("not_before_at IS NULL OR not_before_at <= ?", Time.current)
        .distinct
        .pluck(:policy_name)
    end

    def self.fetch_batch(policy)
      if policy.round_robin?
        if policy.round_robin_weight == :time
          fetch_time_weighted_batch(policy)
        else
          fetch_round_robin_batch(policy)
        end
      else
        fetch_plain_batch(policy)
      end
    end

    def self.fetch_plain_batch(policy)
      StagedJob.pending
        .where(policy_name: policy.name)
        .where("not_before_at IS NULL OR not_before_at <= ?", Time.current)
        .order(:priority, :staged_at)
        .limit(DispatchPolicy.config.batch_size)
        .lock("FOR UPDATE SKIP LOCKED")
        .to_a
    end

    def self.fetch_round_robin_batch(policy)
      quantum    = DispatchPolicy.config.round_robin_quantum
      batch_size = DispatchPolicy.config.batch_size
      now        = Time.current

      sql = <<~SQL.squish
        SELECT rows.*
        FROM (
          SELECT DISTINCT round_robin_key
          FROM dispatch_policy_staged_jobs
          WHERE policy_name = ?
            AND admitted_at IS NULL
            AND round_robin_key IS NOT NULL
            AND (not_before_at IS NULL OR not_before_at <= ?)
        ) AS keys
        CROSS JOIN LATERAL (
          SELECT *
          FROM dispatch_policy_staged_jobs
          WHERE policy_name = ?
            AND admitted_at IS NULL
            AND round_robin_key = keys.round_robin_key
            AND (not_before_at IS NULL OR not_before_at <= ?)
          ORDER BY priority, staged_at
          LIMIT ?
          FOR UPDATE SKIP LOCKED
        ) AS rows
        LIMIT ?
      SQL

      batch = StagedJob.find_by_sql([ sql, policy.name, now, policy.name, now, quantum, batch_size ])

      remaining = batch_size - batch.size
      return batch if remaining <= 0

      top_up = StagedJob.pending
        .where(policy_name: policy.name)
        .where("not_before_at IS NULL OR not_before_at <= ?", now)
        .where.not(id: batch.map(&:id))
        .order(:priority, :staged_at)
        .limit(remaining)
        .lock("FOR UPDATE SKIP LOCKED")
        .to_a

      batch + top_up
    end

    # Time-weighted variant of round-robin: instead of an equal quantum
    # per active partition, allocate quanta proportional to the inverse
    # of recently-consumed compute time. Solo partitions get the full
    # batch_size; competing partitions get slices that bias admission
    # toward whoever has consumed less, so total compute time stays
    # balanced even when one tenant's backlog is much bigger than
    # another's. Falls back to the same trailing top-up as the equal
    # round-robin so we never under-fill the batch when only a few
    # partitions are active.
    DEFAULT_TIME_SHARE_DURATION_MS = 100

    def self.fetch_time_weighted_batch(policy)
      batch_size = DispatchPolicy.config.batch_size
      now        = Time.current

      partitions = StagedJob.pending
        .where(policy_name: policy.name)
        .where("not_before_at IS NULL OR not_before_at <= ?", now)
        .where.not(round_robin_key: nil)
        .distinct
        .pluck(:round_robin_key)

      return fetch_plain_batch(policy) if partitions.empty?

      consumed = PartitionObservation.consumed_ms_by_partition(
        policy_name:    policy.name,
        partition_keys: partitions,
        window:         policy.round_robin_window
      )

      # Inverse-of-consumed weights, with a floor so a brand-new partition
      # (no observations) doesn't dominate to infinity.
      weights = partitions.each_with_object({}) do |key, acc|
        consumed_ms     = consumed.dig(key, :consumed_ms) || 0
        denom           = [ consumed_ms, DEFAULT_TIME_SHARE_DURATION_MS ].max
        acc[key]        = 1.0 / denom
      end
      total_weight = weights.values.sum
      quanta = weights.transform_values do |w|
        [ (batch_size * w / total_weight).floor, 1 ].max
      end

      # Drive a single LATERAL fetch from a VALUES list of
      # (round_robin_key, quantum) pairs. One round-trip regardless of
      # partition count — earlier versions issued one query per
      # partition, which became the inner loop for tenants with
      # thousands of partitions.
      values_clause = ([ "(?, ?::int)" ] * quanta.size).join(", ")
      values_args   = quanta.flat_map { |key, q| [ key, q ] }

      sql = <<~SQL.squish
        WITH plan(round_robin_key, quantum) AS (VALUES #{values_clause})
        SELECT rows.*
        FROM plan
        CROSS JOIN LATERAL (
          SELECT *
          FROM dispatch_policy_staged_jobs
          WHERE policy_name = ?
            AND admitted_at IS NULL
            AND round_robin_key = plan.round_robin_key
            AND (not_before_at IS NULL OR not_before_at <= ?)
          ORDER BY priority, staged_at
          LIMIT plan.quantum
          FOR UPDATE SKIP LOCKED
        ) AS rows
        LIMIT ?
      SQL

      batch = StagedJob.find_by_sql([ sql, *values_args, policy.name, now, batch_size ])

      remaining = batch_size - batch.size
      return batch if remaining <= 0 || batch.empty?

      top_up = StagedJob.pending
        .where(policy_name: policy.name)
        .where("not_before_at IS NULL OR not_before_at <= ?", now)
        .where.not(id: batch.map(&:id))
        .order(:priority, :staged_at)
        .limit(remaining)
        .lock("FOR UPDATE SKIP LOCKED")
        .to_a

      batch + top_up
    end

    def self.lookup_policy(policy_name)
      job_class = DispatchPolicy.registry[policy_name] || autoload_job_for(policy_name)
      return nil unless job_class
      job_class.resolved_dispatch_policy
    end

    def self.autoload_job_for(policy_name)
      const_name = policy_name.tr("-", "/").camelize
      const_name.safe_constantize
      DispatchPolicy.registry[policy_name]
    end

    def self.run_policy(policy, batch)
      context = DispatchContext.new(policy: policy, batch: batch)
      survivors = batch
      policy.gates.each do |gate|
        survivors = gate.filter(survivors, context)
      end

      survivors.map do |staged|
        partitions = context.partitions_for(staged)

        partitions.each do |gate_name, partition_key|
          gate = policy.gates.find { |g| g.name == gate_name.to_sym }
          next unless gate&.tracks_inflight?

          PartitionInflightCount.increment(
            policy_name:   policy.name,
            gate_name:     gate_name.to_s,
            partition_key: partition_key.to_s
          )
        end

        job = staged.mark_admitted!(partitions: partitions)
        [ staged, job ]
      end
    end
  end
end
