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

      ActiveSupport::Notifications.instrument(
        "tick.dispatch_policy",
        policy_name: policy_name
      ) do |payload|
        admitted = 0
        partitions_seen = 0

        begin
          StagedJob.transaction do
            active_policies(policy_name).each do |pname|
              policy = lookup_policy(pname)
              next unless policy

              batch = PolicyRunner.fetch_batch(policy)
              next if batch.empty?

              partitions_seen += batch.map(&:round_robin_key).compact.uniq.size

              pairs = PolicyRunner.run_policy(policy, batch)
              jobs  = pairs.map { |_staged, job| job }

              # Bulk hand-off to the adapter in a single round-trip.
              # GoodJob and Solid Queue override enqueue_all to do one
              # INSERT with N rows; adapters without an override fall
              # back to a per-job enqueue loop. Routes through
              # ActiveJob.perform_all_later so the dispatch by queue_
              # adapter is consistent with the rest of the host app.
              # ActiveJobPerformAllLaterPatch sees _dispatch_admitted_at
              # is already set on these jobs and skips the staging
              # branch, going straight to the adapter.
              ActiveJob.perform_all_later(jobs)

              # ActiveJob adapters can report failure by setting
              # enqueue_error and leaving successfully_enqueued? false
              # instead of raising. Treat that as a hard failure and
              # roll back the entire transaction so admission, counters
              # and the adapter rows all unwind together.
              jobs.each do |job|
                next if job.successfully_enqueued?
                raise EnqueueDeclined,
                  "adapter declined active_job_id=#{job.job_id}: " \
                  "#{job.enqueue_error&.class}: #{job.enqueue_error&.message}"
              end

              admitted += jobs.size
            end
          end
        rescue EnqueueDeclined => e
          Rails.logger&.warn("[DispatchPolicy] #{e.message} — transaction rolled back")
          payload[:declined] = true
          payload[:admitted] = 0
          payload[:partitions] = partitions_seen
          return 0
        end

        payload[:admitted] = admitted
        payload[:partitions] = partitions_seen
        admitted
      end
    end

    # Default batch size for the prune deletes. Keeps any single
    # statement bounded so we don't lock or block writers when the
    # table has many candidate rows. Overridable via config if you
    # need to tune for an unusually high or low churn rate.
    PRUNE_BATCH_SIZE = 5_000

    def self.prune_idle_partitions
      ttl = DispatchPolicy.config.partition_idle_ttl
      return if ttl.nil? || ttl <= 0

      cutoff = Time.current - ttl
      delete_in_batches(PartitionInflightCount.where(in_flight: 0).where("updated_at < ?", cutoff))
      delete_in_batches(ThrottleBucket.where("tokens <= ? AND refilled_at < ?", THROTTLE_ZERO_THRESHOLD, cutoff))
      # Only prune drained partitions (no pending rows). A partition
      # whose pending_count > 0 stays even if last_admitted_at is old
      # — those are partitions waiting their LRU turn.
      delete_in_batches(
        PartitionState.where(pending_count: 0)
                      .where("last_admitted_at IS NULL OR last_admitted_at < ?", cutoff)
      )

      prune_drained_partition_states
    end

    # Aggressive purge: when partition_states accumulates a high
    # proportion of drained (pending_count = 0) rows, delete them
    # regardless of last_admitted_at age. Cheap insurance against
    # policies that churn through many short-lived partition keys
    # — without this, the table can keep many drained rows for up
    # to partition_idle_ttl even when they outnumber active ones.
    def self.prune_drained_partition_states
      threshold = DispatchPolicy.config.partition_drained_purge_threshold
      min_total = DispatchPolicy.config.partition_drained_purge_min_total.to_i
      return if threshold.nil? || threshold <= 0

      total = PartitionState.count
      return if total < min_total

      drained = PartitionState.where(pending_count: 0).count
      return if total.zero? || (drained.to_f / total) < threshold

      delete_in_batches(PartitionState.where(pending_count: 0))
    end

    # Delete in primary-key-paginated batches so a single statement
    # never locks the whole result set. PG `in_batches` walks `id`
    # ranges and yields a relation per chunk; deleting the chunk
    # then moving on is the standard Rails pattern for bulk delete.
    def self.delete_in_batches(scope, batch_size: PRUNE_BATCH_SIZE)
      total = 0
      scope.in_batches(of: batch_size) { |relation| total += relation.delete_all }
      total
    end

    def self.prune_orphan_gate_rows
      [ PartitionInflightCount, ThrottleBucket ].each do |model|
        model.distinct.pluck(:policy_name, :gate_name).each do |policy_name, gate_name|
          policy = lookup_policy(policy_name)
          next if policy && policy.gates.any? { |g| g.name == gate_name.to_sym }

          delete_in_batches(model.where(policy_name: policy_name, gate_name: gate_name))
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
      ActiveSupport::Notifications.instrument("reap.dispatch_policy") do |payload|
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
        if expired.empty?
          payload[:reaped] = 0
          next 0
        end

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

        if deltas.empty?
          payload[:reaped] = expired.size
          next expired.size
        end

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
        PartitionInflightCount.connection.clear_query_cache

        payload[:reaped] = expired.size
        expired.size
      end
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

  end
end

require "dispatch_policy/tick/policy_runner"
