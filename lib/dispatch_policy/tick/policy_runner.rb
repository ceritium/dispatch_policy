# frozen_string_literal: true

module DispatchPolicy
  class Tick
    # Per-policy admission logic. Tick.run iterates active_policies
    # and delegates the fetch + gate + admit + counter work for each
    # one to PolicyRunner — that keeps Tick focused on orchestration
    # (transaction scope, adapter handoff, error handling) and this
    # module focused on the SQL shapes specific to one policy's
    # round-robin / time-weighted / plain fetch path.
    #
    # All methods are class methods so the runner can be called
    # without instantiation; there's no per-policy state worth
    # caching across ticks (gate filters and counter updates all
    # happen inside the staging transaction).
    module PolicyRunner
      module_function

      # Floor on per-partition consumed time for the time-weighted
      # round-robin allocator. Without it, a never-observed partition
      # divides by zero and dominates the batch.
      DEFAULT_TIME_SHARE_DURATION_MS = 100

      # Dispatch fetch shape based on the policy's round_robin
      # declaration. The plain path covers policies without rotation;
      # the round_robin and time-weighted paths drive a LATERAL fetch
      # from a partition cursor pulled via partition_states.
      def fetch_batch(policy)
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

      def fetch_plain_batch(policy)
        StagedJob.pending
          .where(policy_name: policy.name)
          .where("not_before_at IS NULL OR not_before_at <= ?", Time.current)
          .order(:priority, :staged_at)
          .limit(policy.effective_batch_size)
          .lock("FOR UPDATE SKIP LOCKED")
          .to_a
      end

      # Pluck active round_robin_keys for `policy`, ordered least-recently-
      # admitted first so high-cardinality policies still rotate through
      # every active partition. NULL last_admitted_at (never-admitted)
      # rows go first by NULLS FIRST.
      #
      # Reads partition_states directly via the partial index on
      # (policy_name, last_admitted_at) WHERE pending_count > 0. Index
      # seek + LIMIT is O(cap) regardless of total partitions known to
      # the policy — sub-millisecond at any N.
      #
      # Always LIMITs the result so a tick can't degrade as N grows.
      # By default the limit is config.batch_size — that's the smallest
      # value that keeps the LATERAL inner full when quantum ≥ 1.
      # Override via config.round_robin_max_partitions_per_tick.
      def pluck_active_partitions(policy, _now)
        cap = policy.effective_round_robin_max_partitions_per_tick

        PartitionState
          .where(policy_name: policy.name)
          .where("pending_count > 0")
          .order(Arel.sql("last_admitted_at ASC NULLS FIRST"))
          .limit(cap.to_i)
          .pluck(:partition_key)
      end

      def fetch_round_robin_batch(policy)
        quantum    = policy.effective_round_robin_quantum
        batch_size = policy.effective_batch_size
        now        = Time.current

        partitions = pluck_active_partitions(policy, now)

        return fetch_plain_batch(policy) if partitions.empty?

        # Drive the LATERAL from a VALUES list of (round_robin_key,
        # quantum) pairs instead of a SELECT DISTINCT subquery. The
        # planner sees a known-size driver and produces a deterministic
        # plan; the SELECT DISTINCT version flipped between sort+unique
        # and hash-agg based on autovacuum'd stats, with 40x runtime
        # variance across runs at 10k partitions.
        values_clause = ([ "(?, ?::int)" ] * partitions.size).join(", ")
        values_args   = partitions.flat_map { |key| [ key, quantum ] }

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

        refill_round_robin(policy, partitions, batch, remaining, now)
      end

      # Round-robin-aware refill: when the first LATERAL pass leaves the
      # batch short of batch_size (some plucked partitions had fewer rows
      # than their quantum), do a second LATERAL pass distributing the
      # remaining slots equally across the SAME plucked partitions. The
      # old FIFO top-up grabbed rows from anywhere ordered by staged_at,
      # which let a hot partition outside the cap'd subset bypass the
      # rotation; this version stays inside the LRU subset.
      def refill_round_robin(policy, partitions, batch, remaining, now)
        return batch if partitions.empty?

        refill_quantum = (remaining.to_f / partitions.size).ceil
        values_clause  = ([ "(?, ?::int)" ] * partitions.size).join(", ")
        values_args    = partitions.flat_map { |k| [ k, refill_quantum ] }

        excluded_ids = batch.map(&:id)
        # IDs are integers from staged_jobs.id (bigint), no SQL-injection
        # risk. Avoids spreading them as binds — there can be batch_size
        # of them and `?` placeholder substitution gets clunky.
        exclude_clause = excluded_ids.empty? ? "" : "AND s.id NOT IN (#{excluded_ids.join(',')})"

        sql = <<~SQL.squish
          WITH plan(round_robin_key, quantum) AS (VALUES #{values_clause})
          SELECT rows.*
          FROM plan
          CROSS JOIN LATERAL (
            SELECT s.*
            FROM dispatch_policy_staged_jobs s
            WHERE s.policy_name = ?
              AND s.admitted_at IS NULL
              AND s.round_robin_key = plan.round_robin_key
              AND (s.not_before_at IS NULL OR s.not_before_at <= ?)
              #{exclude_clause}
            ORDER BY s.priority, s.staged_at
            LIMIT plan.quantum
            FOR UPDATE SKIP LOCKED
          ) AS rows
          LIMIT ?
        SQL

        refill = StagedJob.find_by_sql([ sql, *values_args, policy.name, now, remaining ])
        batch + refill
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
      def fetch_time_weighted_batch(policy)
        batch_size = policy.effective_batch_size
        now        = Time.current

        partitions = pluck_active_partitions(policy, now)

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

        refill_round_robin(policy, partitions, batch, remaining, now)
      end

      # Apply gates, then mark survivors admitted and bump the relevant
      # counter rows in bulk. Returns [[staged, active_job], ...] pairs
      # so Tick.run can hand them off to ActiveJob.perform_all_later.
      def run_policy(policy, batch)
        context = DispatchContext.new(policy: policy, batch: batch)
        survivors = batch
        policy.gates.each do |gate|
          survivors = gate.filter(survivors, context)
        end

        return [] if survivors.empty?

        now           = Time.current
        lease_expires = now + policy.effective_lease_duration
        gate_index    = policy.gates.each_with_object({}) { |g, h| h[g.name.to_s] = g }

        staged_updates  = []
        counter_deltas  = Hash.new(0)
        admit_counts    = Hash.new(0)
        pairs           = []

        survivors.each do |staged|
          partitions = context.partitions_for(staged)

          partitions.each do |gate_name, partition_key|
            gate = gate_index[gate_name.to_s]
            next unless gate&.tracks_inflight?
            counter_deltas[[ policy.name, gate_name.to_s, partition_key.to_s ]] += 1
          end

          # Track per-round_robin_key admissions for the LRU + count
          # decrement on partition_states. Skip rows without a
          # round_robin_key (plain policies don't need rotation and
          # don't get tracked in partition_states).
          admit_counts[staged.round_robin_key] += 1 if staged.round_robin_key

          job = staged.instantiate_active_job
          job._dispatch_partitions  = partitions
          job._dispatch_admitted_at = now

          staged_updates << [ staged.id, job.job_id, partitions ]
          pairs          << [ staged, job ]
        end

        StagedJob.mark_admitted_many!(
          rows:             staged_updates,
          admitted_at:      now,
          lease_expires_at: lease_expires
        )
        PartitionInflightCount.increment_many!(deltas: counter_deltas)
        PartitionState.admit_many!(
          policy_name: policy.name,
          counts:      admit_counts,
          now:         now
        )

        pairs
      end
    end
  end
end
