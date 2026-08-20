# frozen_string_literal: true

require "json"

module DispatchPolicy
  # SQL access layer for staged_jobs / partitions / inflight_jobs.
  #
  # Hot paths use raw SQL via ActiveRecord::Base.connection so we get
  # `FOR UPDATE SKIP LOCKED`, multi-row UPSERTs, and DELETE … RETURNING
  # without ActiveRecord overhead. Read paths in the engine UI use the
  # AR models in app/models/dispatch_policy/*.
  module Repository
    STAGED_TABLE      = "dispatch_policy_staged_jobs"
    PARTITIONS_TABLE  = "dispatch_policy_partitions"
    INFLIGHT_TABLE    = "dispatch_policy_inflight_jobs"
    SAMPLES_TABLE        = "dispatch_policy_tick_samples"
    ADAPTIVE_TABLE       = "dispatch_policy_adaptive_concurrency_stats"
    POLICY_SETTINGS_TABLE = "dispatch_policy_policy_settings"

    # Every table the gem owns, i.e. everything the single migration
    # creates. The canonical list: the test bootstrap and the benchmark
    # harness both build their schema/truncate/drop statements from this
    # instead of keeping hand-synced copies, which is how a new table
    # used to be missed in one of them (a stale table then fails the next
    # `recreate_schema!` with DuplicateTable, and a table missing from a
    # truncate leaks state into the following test). Adding a table means
    # adding it here — see the "Adding a table?" workflow in CLAUDE.md.
    ALL_TABLES = [
      STAGED_TABLE,
      PARTITIONS_TABLE,
      INFLIGHT_TABLE,
      SAMPLES_TABLE,
      ADAPTIVE_TABLE,
      POLICY_SETTINGS_TABLE
    ].freeze

    module_function

    def connection
      ActiveRecord::Base.connection
    end

    # Wraps `block` in `connected_to(role: …)` when DispatchPolicy.config
    # .database_role is set. Used by Tick to ensure the admission TX is
    # opened against the same DB role that good_job / solid_queue uses,
    # critical for multi-DB Rails setups (e.g. solid_queue on a separate
    # `:queue` DB) where atomicity only holds when the staging TX and the
    # adapter INSERT share a connection.
    def with_connection
      role = DispatchPolicy.config.database_role
      if role && ActiveRecord::Base.respond_to?(:connected_to)
        ActiveRecord::Base.connected_to(role: role) { yield }
      else
        yield
      end
    end

    # ----- staging (write path) ------------------------------------------------

    # Insert one staged_job row + UPSERT its partition. The partition's
    # `context` is refreshed on every call so admission-time gates always
    # see the latest dynamic config.
    #
    # @param policy_name   [String]
    # @param partition_key [String]
    # @param queue_name    [String, nil]
    # @param job_class     [String]
    # @param job_data      [Hash]
    # @param context       [Hash]
    # @param scheduled_at  [Time, nil]
    # @param priority      [Integer]
    def stage!(policy_name:, partition_key:, queue_name:, job_class:, job_data:, context:,
               shard: Policy::DEFAULT_SHARD, scheduled_at: nil, priority: 0)
      connection.transaction(requires_new: true) do
        connection.exec_query(
          <<~SQL.squish,
            INSERT INTO #{STAGED_TABLE}
              (policy_name, partition_key, queue_name, job_class, job_data, context, scheduled_at, priority, enqueued_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, now())
          SQL
          "stage_job",
          [policy_name, partition_key, queue_name, job_class, JSON.dump(job_data), JSON.dump(context), scheduled_at, priority]
        )
        upsert_partition!(
          policy_name:   policy_name,
          partition_key: partition_key,
          queue_name:    queue_name,
          shard:         shard,
          context:       context,
          delta_pending: 1
        )
      end
      true
    end

    # Bulk version for perform_all_later. Receives an array of hashes with
    # the same keys as #stage!. Performs one INSERT for staged_jobs and
    # one UPSERT per (policy_name, partition_key) group.
    # Rows per INSERT. Each row binds 8 params; Postgres caps a statement at
    # 65_535 bind params, so we slice well under 65_535/8 ≈ 8_191 to leave
    # headroom. A single perform_all_later with more rows than this would
    # otherwise blow the limit and fail the whole batch.
    STAGE_MANY_BATCH = 1_000

    def stage_many!(rows)
      return 0 if rows.empty?

      connection.transaction(requires_new: true) do
        rows.each_slice(STAGE_MANY_BATCH) do |slice|
          values_sql = []
          params     = []
          slice.each_with_index do |row, idx|
            base = idx * 8
            values_sql << "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}, $#{base + 5}::jsonb, $#{base + 6}::jsonb, $#{base + 7}, $#{base + 8})"
            params.push(
              row[:policy_name],
              row[:partition_key],
              row[:queue_name],
              row[:job_class],
              JSON.dump(row[:job_data]),
              JSON.dump(row[:context] || {}),
              row[:scheduled_at],
              row[:priority] || 0
            )
          end
          connection.exec_query(
            <<~SQL.squish,
              INSERT INTO #{STAGED_TABLE}
                (policy_name, partition_key, queue_name, job_class, job_data, context, scheduled_at, priority)
              VALUES #{values_sql.join(", ")}
            SQL
            "stage_many",
            params
          )
        end

        # Sorted so concurrent bulk enqueues touching the same partitions
        # take their row locks in the same order. Two perform_all_later
        # calls that happened to list partitions A,B and B,A could
        # otherwise each hold one and wait for the other — a deadlock
        # Postgres resolves by killing one of the transactions, losing
        # that whole batch's staging.
        rows.group_by { |r| [r[:policy_name], r[:partition_key]] }.sort_by(&:first).each do |(policy_name, partition_key), group|
          upsert_partition!(
            policy_name:   policy_name,
            partition_key: partition_key,
            queue_name:    group.first[:queue_name],
            shard:         group.first[:shard] || Policy::DEFAULT_SHARD,
            context:       group.last[:context] || {},
            delta_pending: group.size
          )
        end
      end
      rows.size
    end

    def upsert_partition!(policy_name:, partition_key:, queue_name:, context:, delta_pending:,
                          shard: Policy::DEFAULT_SHARD)
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{PARTITIONS_TABLE}
            (policy_name, partition_key, queue_name, shard, context, context_updated_at,
             pending_count, last_enqueued_at, status, gate_state, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5::jsonb, now(), $6, now(), 'active', '{}'::jsonb, now(), now())
          ON CONFLICT (policy_name, partition_key) DO UPDATE SET
            context             = EXCLUDED.context,
            context_updated_at  = EXCLUDED.context_updated_at,
            queue_name          = COALESCE(EXCLUDED.queue_name, #{PARTITIONS_TABLE}.queue_name),
            shard               = #{PARTITIONS_TABLE}.shard,
            pending_count       = #{PARTITIONS_TABLE}.pending_count + EXCLUDED.pending_count,
            last_enqueued_at    = EXCLUDED.last_enqueued_at,
            updated_at          = now()
        SQL
        "upsert_partition",
        [policy_name, partition_key, queue_name, shard, JSON.dump(context), delta_pending]
      )
    end

    # ----- tick path -----------------------------------------------------------

    # Lock + return up to `limit` partitions ready to be evaluated by the tick.
    # Each row's last_checked_at is bumped to now() so the next tick fairly
    # picks others. Locked rows are released when the transaction commits.
    #
    # When `shard` is non-nil, only partitions on that shard are claimed —
    # this lets several tick processes work on the same policy in parallel,
    # one per shard.
    def claim_partitions(policy_name:, limit:, shard: nil)
      params      = [policy_name]
      shard_sql   = ""
      if shard
        params    << shard
        shard_sql = " AND shard = $#{params.size}"
      end
      params << limit

      sql = <<~SQL.squish
        WITH candidates AS (
          SELECT id FROM #{PARTITIONS_TABLE}
          WHERE policy_name = $1
            AND status = 'active'
            AND pending_count > 0
            AND (next_eligible_at IS NULL OR next_eligible_at <= now())
            AND NOT EXISTS (
              SELECT 1 FROM #{POLICY_SETTINGS_TABLE} ps
              WHERE ps.policy_name = $1 AND ps.paused
            )
            #{shard_sql}
          ORDER BY last_checked_at NULLS FIRST, id
          LIMIT $#{params.size}
          FOR UPDATE SKIP LOCKED
        )
        UPDATE #{PARTITIONS_TABLE} p
        SET last_checked_at = now()
        FROM candidates
        WHERE p.id = candidates.id
        RETURNING p.*
      SQL
      result = connection.exec_query(sql, "claim_partitions", params)
      result.to_a.map { |row| normalize_partition(row) }
    end

    # Atomically claim up to `limit` staged rows for a partition (DELETE …
    # RETURNING) and update the partition's counters / gate_state /
    # next_eligible_at in the same transaction.
    #
    # `limit` MUST be positive: the deny path (no rows to admit) goes
    # through `bulk_record_partition_denies!` instead, which collapses
    # many partitions into a single UPDATE…FROM(VALUES…) at the end of
    # the tick.
    def claim_staged_jobs!(policy_name:, partition_key:, limit:, retry_after:,
                           gate_state_patch: nil, half_life_seconds: nil,
                           preserve_next_eligible: false, throttle_charge: nil)
      raise ArgumentError, "claim_staged_jobs! requires limit > 0" unless limit.positive?

      sql_select = <<~SQL.squish
        WITH claimed AS (
          SELECT id FROM #{STAGED_TABLE}
          WHERE policy_name = $1 AND partition_key = $2
            AND (scheduled_at IS NULL OR scheduled_at <= now())
          ORDER BY priority DESC, scheduled_at NULLS FIRST, id
          LIMIT $3
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM #{STAGED_TABLE} s
        USING claimed
        WHERE s.id = claimed.id
        RETURNING s.*
      SQL
      rows = connection.exec_query(sql_select, "claim_staged_jobs", [policy_name, partition_key, limit]).to_a

      # The gate_state patch may depend on how many rows we actually
      # claimed (e.g. the throttle charges its bucket for jobs admitted,
      # not for the optimistic `allowed`). When the caller passes a block
      # it receives that real count and returns the patch to persist;
      # gate-less callers pass a fixed `gate_state_patch:` instead.
      patch = block_given? ? yield(rows.size) : (gate_state_patch || {})

      record_partition_admit!(
        policy_name:       policy_name,
        partition_key:     partition_key,
        admitted:          rows.size,
        gate_state_patch:  patch,
        retry_after:       retry_after,
        half_life_seconds: half_life_seconds,
        preserve_next_eligible: preserve_next_eligible,
        throttle_charge:   throttle_charge
      )

      rows.map { |r| normalize_staged(r) }
    end

    # Per-partition admit-state UPDATE. Runs inside the per-partition
    # admission TX alongside the DELETE, so pending_count / total_admitted
    # / gate_state changes commit atomically with the claim and the
    # adapter handoff. For the deny case use `bulk_record_partition_denies!`.
    #
    # When `half_life_seconds` is non-nil, the row's EWMA decayed_admits
    # counter is also refreshed in the same UPDATE: previous value
    # decays exponentially based on the elapsed wall time since the
    # last update, then `admitted` is added on top. This keeps fairness
    # state atomic with the admit (no separate write, no race) and
    # leaves the partitions row's lock undisturbed.
    # `preserve_next_eligible` leaves any existing backoff alone instead of
    # replacing it. The Tick wants the default: it has just evaluated the
    # gates, so what they said supersedes whatever was there. A forced
    # admission (the UI's admit/drain) bypassed the gates entirely and has
    # therefore learned nothing about capacity — clearing the backoff there
    # just makes the next tick re-claim the partition, re-evaluate it and
    # back it off again.
    # `throttle_charge` — {capacity:, refill_rate:} — makes the token
    # bucket settle IN this UPDATE, from the row's own current value,
    # instead of writing back a number Ruby computed from a read that
    # happened earlier. That read-modify-write was a real hole: two tick
    # loops covering the same (policy, shard) each evaluated a full
    # bucket, each admitted it, and the second write simply overwrote the
    # first — the second admission was never charged, so the effective
    # rate became rate x loops, indefinitely. Computed here, the two
    # charges compose: the bucket goes negative and the debt is repaid
    # out of the next window, so the long-run rate holds. (The transient
    # burst is still possible — the ADMISSION decision is not
    # serialised, only the charge — which is why one tick loop per
    # (policy, shard) remains the recommended setup.)
    def record_partition_admit!(policy_name:, partition_key:, admitted:, gate_state_patch:,
                                retry_after:, half_life_seconds: nil,
                                preserve_next_eligible: false,
                                throttle_charge: nil)
      next_eligible_sql, next_eligible_params =
        if preserve_next_eligible
          ["next_eligible_at", []]
        else
          next_eligible_clause(retry_after)
        end
      gate_state_json = JSON.dump(gate_state_patch || {})

      params = [policy_name, partition_key, admitted, gate_state_json, *next_eligible_params]

      if half_life_seconds && half_life_seconds.to_f.positive?
        # decay constant τ such that exp(-Δt/τ) halves every half_life:
        # τ = half_life / ln(2). NULLIF guards a degenerate τ=0.
        #
        # The GREATEST(..., -700) clamp keeps `exp()` from raising
        # `value out of range: underflow` when a partition has been
        # idle for many half-lives. Postgres throws around
        # `exp(-746)` on double precision; -700 still yields a finite
        # ~9.86e-305, which is effectively zero for the EWMA. Without
        # the clamp, a partition idle long enough for Δt/τ to exceed
        # ~746 breaks every subsequent admission UPDATE on it: Tick
        # rolls back the whole TX, the staged rows return, and the
        # partition never drains.
        decay_idx        = params.size + 1
        admitted_idx_for_ewma = 3
        decay_tau        = half_life_seconds.to_f / Math.log(2)
        params << decay_tau
        decay_sql = <<~SQL.squish
          decayed_admits     = decayed_admits *
                                exp(GREATEST(
                                  - COALESCE(EXTRACT(EPOCH FROM (now() - decayed_admits_at)), 0)
                                    / NULLIF($#{decay_idx}::double precision, 0),
                                  -700
                                ))
                              + $#{admitted_idx_for_ewma},
          decayed_admits_at  = now(),
        SQL
      else
        decay_sql = ""
      end

      # Recompute the bucket from the row: refill by the time elapsed
      # since ITS refilled_at, clamp to capacity, then subtract what we
      # actually admitted. Deliberately NOT floored at zero — a negative
      # balance is how a concurrent over-admission gets repaid instead of
      # forgiven, and `evaluate` treats anything under one whole token as
      # empty. Built with `||` on top of the literal patch, so it wins for
      # the "throttle" key while other gates' keys survive; every
      # gate_state reference below reads the pre-UPDATE row.
      if throttle_charge
        cap_idx  = params.size + 1
        rate_idx = params.size + 2
        params << throttle_charge.fetch(:capacity).to_f
        params << throttle_charge.fetch(:refill_rate).to_f
        gate_state_sql = <<~SQL.squish
          (gate_state || $4::jsonb) || jsonb_build_object('throttle', jsonb_build_object(
            'tokens', LEAST(
                COALESCE((gate_state -> 'throttle' ->> 'tokens')::double precision, $#{cap_idx}::double precision)
                + GREATEST(
                    EXTRACT(EPOCH FROM now())
                    - COALESCE((gate_state -> 'throttle' ->> 'refilled_at')::double precision,
                               EXTRACT(EPOCH FROM now())),
                    0
                  ) * $#{rate_idx}::double precision,
                $#{cap_idx}::double precision
              ) - $3,
            'refilled_at', EXTRACT(EPOCH FROM now())
          ))
        SQL
      else
        gate_state_sql = "gate_state || $4::jsonb"
      end

      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{PARTITIONS_TABLE}
          SET pending_count    = GREATEST(pending_count - $3, 0),
              total_admitted   = total_admitted + $3,
              last_admit_at    = CASE WHEN $3 > 0 THEN now() ELSE last_admit_at END,
              gate_state       = #{gate_state_sql},
              next_eligible_at = #{next_eligible_sql},
              #{decay_sql}
              updated_at       = now()
          WHERE policy_name = $1 AND partition_key = $2
        SQL
        "record_partition_admit",
        params
      )
    end

    # Park a partition until its soonest future-scheduled job is due.
    #
    # `claim_partitions` selects on `pending_count > 0`, which counts rows
    # scheduled for later, while `claim_staged_jobs!` only takes rows whose
    # `scheduled_at` has arrived. A partition holding nothing but future
    # work therefore claims, finds nothing, and — with `next_eligible_at`
    # left NULL — is immediately eligible again: a full transaction and a
    # `partition_batch_size` slot burned every tick until the job is due,
    # with `no_rows_claimed` filling the denial breakdown meanwhile.
    #
    # Only sets the value when there is no backoff already: a gate that
    # just asked for one is asking about capacity, which outranks this.
    # A NULL result (another tick took the rows in between) leaves the
    # partition immediately eligible, which is correct.
    def defer_partition_to_next_scheduled!(policy_name:, partition_key:)
      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{PARTITIONS_TABLE} p
          SET next_eligible_at = (
                SELECT MIN(s.scheduled_at) FROM #{STAGED_TABLE} s
                WHERE s.policy_name = $1 AND s.partition_key = $2
                  AND s.scheduled_at > now()
              ),
              updated_at = now()
          WHERE p.policy_name = $1 AND p.partition_key = $2
            AND p.next_eligible_at IS NULL
        SQL
        "defer_partition_to_next_scheduled",
        [policy_name, partition_key]
      )
    end

    # Bulk-update many partitions whose pipeline this tick decided to deny.
    # One UPDATE…FROM(VALUES…) instead of one UPDATE per partition, which
    # cuts a tick with `partition_batch_size = 50` from ~50 round-trips on
    # the deny path to one. The deny path doesn't touch pending_count or
    # total_admitted (admitted = 0 makes them no-ops in the per-row
    # UPDATE), so we only write gate_state and next_eligible_at here.
    #
    # Each entry: { policy_name:, partition_key:, gate_state_patch:, retry_after: }.
    # Independent per row — the join via FROM(VALUES…) makes the bulk
    # statement equivalent to N sequential UPDATEs in correctness terms.
    # Note: `claim_partitions` runs as its own autocommitted statement, so
    # its `FOR UPDATE SKIP LOCKED` row locks are already released by the time
    # we reach this flush — they do NOT guard the batch. What keeps two ticks
    # off the same partitions is the operational invariant of one tick loop
    # per (policy, shard), reinforced by the `last_checked_at` bump on claim
    # (a racing claim skips recently-checked rows).
    def bulk_record_partition_denies!(entries)
      return if entries.empty?

      values_sql = []
      params     = []
      entries.each_with_index do |e, idx|
        base = idx * 4
        values_sql << "($#{base + 1}::text, $#{base + 2}::text, $#{base + 3}::jsonb, $#{base + 4}::numeric)"
        params.push(
          e[:policy_name],
          e[:partition_key],
          JSON.dump(e[:gate_state_patch] || {}),
          e[:retry_after].nil? ? nil : e[:retry_after].to_f.round(3)
        )
      end

      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{PARTITIONS_TABLE} p
          SET gate_state       = p.gate_state || v.gate_state_patch,
              next_eligible_at = CASE
                WHEN v.retry_after_secs IS NULL THEN p.next_eligible_at
                ELSE now() + (v.retry_after_secs || ' seconds')::interval
              END,
              updated_at       = now()
          FROM (VALUES #{values_sql.join(", ")})
            AS v(policy_name, partition_key, gate_state_patch, retry_after_secs)
          WHERE p.policy_name = v.policy_name AND p.partition_key = v.partition_key
        SQL
        "bulk_record_partition_denies",
        params
      )
    end

    # ----- policy settings ------------------------------------------------------

    # Upsert the pause flag for a policy. The tick's claim_partitions reads
    # this row, so toggling it takes effect for every partition of the
    # policy — including ones created after the toggle.
    def set_policy_paused!(policy_name:, paused:)
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{POLICY_SETTINGS_TABLE} (policy_name, paused, created_at, updated_at)
          VALUES ($1, $2, now(), now())
          ON CONFLICT (policy_name)
          DO UPDATE SET paused = EXCLUDED.paused, updated_at = now()
        SQL
        "set_policy_paused",
        [policy_name, paused ? true : false]
      )
    end

    # ----- inflight tracking ---------------------------------------------------

    def insert_inflight!(rows)
      return if rows.empty?

      values_sql = []
      params     = []
      rows.each_with_index do |row, idx|
        base = idx * 3
        values_sql << "($#{base + 1}, $#{base + 2}, $#{base + 3}, now(), now())"
        params.push(row[:policy_name], row[:partition_key], row[:active_job_id])
      end
      # ON CONFLICT (active_job_id) DO NOTHING covers two paths that
      # the around_perform tracker exercises on its own:
      #   1) the around_perform inflight insert runs even when the row
      #      was already pre-inserted by Tick (concurrency-gated policies);
      #   2) a stale row that survived a crash gets re-inserted by the
      #      around_perform without colliding while the sweeper is still
      #      catching up.
      # Admission proper can no longer collide here: Tick regenerates
      # active_job_id before this insert, so each admission contributes a
      # fresh UUID.
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{INFLIGHT_TABLE}
            (policy_name, partition_key, active_job_id, admitted_at, heartbeat_at)
          VALUES #{values_sql.join(", ")}
          ON CONFLICT (active_job_id) DO NOTHING
        SQL
        "insert_inflight",
        params
      )
    end

    def delete_inflight!(active_job_id:)
      connection.exec_query(
        "DELETE FROM #{INFLIGHT_TABLE} WHERE active_job_id = $1",
        "delete_inflight",
        [active_job_id]
      )
    end

    def heartbeat_inflight!(active_job_id:)
      connection.exec_query(
        "UPDATE #{INFLIGHT_TABLE} SET heartbeat_at = now() WHERE active_job_id = $1",
        "heartbeat_inflight",
        [active_job_id]
      )
    end

    def count_inflight(policy_name:, partition_key:)
      result = connection.exec_query(
        "SELECT count(*)::int AS n FROM #{INFLIGHT_TABLE} WHERE policy_name = $1 AND partition_key = $2",
        "count_inflight",
        [policy_name, partition_key]
      )
      Integer(result.rows.first.first)
    end

    # Reap inflight rows whose owner is gone. Two tiers, distinguished by
    # whether the row was ever heartbeated past its admission:
    #
    #   heartbeat_at > admitted_at  → the worker started performing and the
    #     heartbeat thread advanced heartbeat_at at least once. If it then
    #     went silent for `cutoff_seconds`, the worker died mid-run: reap.
    #
    #   heartbeat_at <= admitted_at → never heartbeated past admission. The
    #     row was pre-inserted by the Tick and the job is still waiting in
    #     the adapter's queue (or only just started — the first heartbeat
    #     fires after inflight_heartbeat_interval). Reaping these at the
    #     short cutoff would under-count the concurrency gate and over-admit
    #     whenever queue latency exceeds it. Only reap once they're older
    #     than the far more generous `queued_cutoff_seconds`, by which point
    #     the admission is presumed lost.
    #
    # The Tick pre-insert writes admitted_at and heartbeat_at from the same
    # now() (a single statement), so a never-started row has them exactly
    # equal; one heartbeat makes heartbeat_at strictly greater.
    def sweep_stale_inflight!(cutoff_seconds:, queued_cutoff_seconds: nil)
      queued_cutoff_seconds ||= cutoff_seconds
      connection.exec_query(
        <<~SQL.squish,
          DELETE FROM #{INFLIGHT_TABLE}
          WHERE (heartbeat_at > admitted_at
                 AND heartbeat_at < now() - ($1 || ' seconds')::interval)
             OR (heartbeat_at <= admitted_at
                 AND admitted_at < now() - ($2 || ' seconds')::interval)
        SQL
        "sweep_stale_inflight",
        [cutoff_seconds.to_i, queued_cutoff_seconds.to_i]
      )
    end

    # Removes partitions that have no pending staged jobs and have been
    # idle for `cutoff_seconds`. The default cutoff (24h) is well past any
    # reasonable inflight job — concurrency state lives in inflight_jobs
    # and is independent of partition rows, so a recreated partition will
    # re-observe the live in-flight count via the concurrency gate.
    # ----- metrics --------------------------------------------------------------

    # Records one row per Tick.run with admission and timing aggregates so the
    # operator UI can display rates over time without sampling on the read
    # path.
    def record_tick_sample!(policy_name:, duration_ms:, partitions_seen:, partitions_admitted:,
                            partitions_denied:, jobs_admitted:, forward_failures:,
                            pending_total:, inflight_total:, denied_reasons:)
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{SAMPLES_TABLE}
            (policy_name, sampled_at, duration_ms, partitions_seen, partitions_admitted,
             partitions_denied, jobs_admitted, forward_failures, pending_total,
             inflight_total, denied_reasons)
          VALUES ($1, now(), $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        SQL
        "record_tick_sample",
        [policy_name, duration_ms.to_i, partitions_seen.to_i, partitions_admitted.to_i,
         partitions_denied.to_i, jobs_admitted.to_i, forward_failures.to_i,
         pending_total.to_i, inflight_total.to_i, JSON.dump(denied_reasons || {})]
      )
    end

    # Aggregate counters since `since` (a Time). If `policy_name` is nil,
    # aggregates across all policies. Returns a Hash with summary keys.
    def tick_summary(policy_name: nil, since:)
      where_sql, params = sample_filter(policy_name, since)
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            COALESCE(SUM(jobs_admitted), 0)::int        AS jobs_admitted,
            COALESCE(SUM(partitions_seen), 0)::int       AS partitions_seen,
            COALESCE(SUM(partitions_admitted), 0)::int   AS partitions_admitted,
            COALESCE(SUM(partitions_denied), 0)::int     AS partitions_denied,
            COALESCE(SUM(forward_failures), 0)::int      AS forward_failures,
            COUNT(*)::int                                AS ticks,
            COALESCE(AVG(duration_ms), 0)::int           AS avg_duration_ms,
            COALESCE(MAX(duration_ms), 0)::int           AS max_duration_ms,
            MAX(sampled_at)                              AS last_sampled_at
          FROM #{SAMPLES_TABLE}
          #{where_sql}
        SQL
        "tick_summary",
        params
      )
      row = result.first || {}
      {
        jobs_admitted:       row["jobs_admitted"].to_i,
        partitions_seen:     row["partitions_seen"].to_i,
        partitions_admitted: row["partitions_admitted"].to_i,
        partitions_denied:   row["partitions_denied"].to_i,
        forward_failures:    row["forward_failures"].to_i,
        ticks:               row["ticks"].to_i,
        avg_duration_ms:     row["avg_duration_ms"].to_i,
        max_duration_ms:     row["max_duration_ms"].to_i,
        last_sampled_at:     row["last_sampled_at"]
      }
    end

    # One grouped query returning per-policy tick aggregates, keyed by
    # policy_name. Replaces calling tick_summary once per policy on the
    # dashboard (N queries → 1). Only the fields the overview renders.
    #   { "policy_a" => { jobs_admitted:, forward_failures:, ticks:,
    #                     avg_duration_ms: }, ... }
    def tick_summaries_by_policy(since:)
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            policy_name,
            COALESCE(SUM(jobs_admitted), 0)::int    AS jobs_admitted,
            COALESCE(SUM(forward_failures), 0)::int AS forward_failures,
            COUNT(*)::int                           AS ticks,
            COALESCE(AVG(duration_ms), 0)::int      AS avg_duration_ms
          FROM #{SAMPLES_TABLE}
          WHERE sampled_at >= $1
          GROUP BY policy_name
        SQL
        "tick_summaries_by_policy",
        [since]
      )
      result.to_a.each_with_object({}) do |r, h|
        h[r["policy_name"]] = {
          jobs_admitted:    r["jobs_admitted"].to_i,
          forward_failures: r["forward_failures"].to_i,
          ticks:            r["ticks"].to_i,
          avg_duration_ms:  r["avg_duration_ms"].to_i
        }
      end
    end

    # Aggregate denied_reasons jsonb across samples in window: returns
    # { "throttle" => 12, "concurrency_full" => 3, ... }
    def denied_reasons_summary(policy_name: nil, since:)
      where_sql, params = sample_filter(policy_name, since)
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT key, SUM(value::int)::int AS total
          FROM #{SAMPLES_TABLE},
               LATERAL jsonb_each_text(denied_reasons)
          #{where_sql}
          GROUP BY key
          ORDER BY total DESC
        SQL
        "denied_reasons_summary",
        params
      )
      result.to_a.each_with_object({}) { |r, h| h[r["key"]] = r["total"].to_i }
    end

    # The single most-denied reason per policy in one query, keyed by
    # policy_name → [reason, count]. Replaces calling denied_reasons_summary
    # per policy on the dashboard just to read its top entry.
    def top_denied_reason_by_policy(since:)
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT DISTINCT ON (policy_name) policy_name, key, total
          FROM (
            SELECT policy_name, key, SUM(value::int)::int AS total
            FROM #{SAMPLES_TABLE},
                 LATERAL jsonb_each_text(denied_reasons)
            WHERE sampled_at >= $1
            GROUP BY policy_name, key
          ) t
          ORDER BY policy_name, total DESC
        SQL
        "top_denied_reason_by_policy",
        [since]
      )
      result.to_a.each_with_object({}) do |r, h|
        h[r["policy_name"]] = [r["key"], r["total"].to_i]
      end
    end

    # Returns time-bucketed series for sparklines. `bucket_seconds` is the
    # bucket width. Each row: { bucket_at:, jobs_admitted:, forward_failures:,
    # pending_total:, ticks: }.
    #
    # `pending_total` is the AVERAGE pending observed across the ticks
    # in that bucket — using AVG (not MAX/last) gives a smoother trend
    # that's resilient to a single outlier sample dragging the bucket up.
    def tick_samples_buckets(policy_name: nil, since:, bucket_seconds: 60)
      where_sql, params = sample_filter(policy_name, since)
      bucket_param_idx = params.size + 1
      params << bucket_seconds.to_i

      # `date_bin` requires Postgres 14+. We compute the bucket via floor on
      # the epoch instead so the gem also runs on Postgres 12/13.
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            to_timestamp(floor(extract(epoch from sampled_at) / $#{bucket_param_idx})::bigint * $#{bucket_param_idx}) AS bucket_at,
            COALESCE(SUM(jobs_admitted), 0)::int AS jobs_admitted,
            COALESCE(SUM(forward_failures), 0)::int AS forward_failures,
            COALESCE(AVG(pending_total), 0)::int AS pending_total,
            COUNT(*)::int AS ticks
          FROM #{SAMPLES_TABLE}
          #{where_sql}
          GROUP BY bucket_at
          ORDER BY bucket_at ASC
        SQL
        "tick_samples_buckets",
        params
      )
      result.to_a.map do |r|
        { bucket_at:        r["bucket_at"],
          jobs_admitted:    r["jobs_admitted"].to_i,
          forward_failures: r["forward_failures"].to_i,
          pending_total:    r["pending_total"].to_i,
          ticks:            r["ticks"].to_i }
      end
    end

    # Direction of a numeric series. Compares the average of the first
    # third to the last third — robust to noise on the ends.
    def self.trend_direction(values, threshold_ratio: 0.10)
      return :flat if values.size < 3

      n      = values.size
      head   = values.first(n / 3)
      tail   = values.last(n / 3)
      head_avg = head.sum.to_f / head.size
      tail_avg = tail.sum.to_f / tail.size

      return :flat if head_avg.zero? && tail_avg.zero?

      delta_ratio = (tail_avg - head_avg) / [head_avg, 1.0].max
      if delta_ratio >= threshold_ratio
        :up
      elsif delta_ratio <= -threshold_ratio
        :down
      else
        :flat
      end
    end

    # Round-trip statistics across active partitions: how stale is the most-
    # stale partition the tick has yet to revisit? P50/P95/oldest ages help
    # decide if partition_batch_size needs to grow or ticks need sharding.
    def partition_round_trip_stats(policy_name: nil)
      filter_sql = "WHERE p.status = 'active' AND p.pending_count > 0"
      params     = []
      if policy_name
        filter_sql += " AND p.policy_name = $1"
        params << policy_name
      end

      # For ages (now - last_checked_at) the percentile direction inverts:
      # the 95th percentile of *age* corresponds to the 5th percentile of the
      # *timestamp* (the oldest 5% of last_checked_at values). Computing the
      # percentile directly on now()-last_checked_at would be cleaner but
      # PostgreSQL's PERCENTILE_DISC needs an ordered set on a column, so we
      # invert the percentile argument instead.
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            COUNT(*)::int AS active_partitions,
            COUNT(*) FILTER (WHERE p.last_checked_at IS NULL)::int AS never_checked,
            COUNT(*) FILTER (WHERE p.next_eligible_at IS NOT NULL AND p.next_eligible_at > now())::int AS in_backoff,
            EXTRACT(EPOCH FROM (now() - MIN(p.last_checked_at)))::float AS oldest_age_seconds,
            EXTRACT(EPOCH FROM (now() - PERCENTILE_DISC(0.5)  WITHIN GROUP (ORDER BY p.last_checked_at)))::float AS p50_age_seconds,
            EXTRACT(EPOCH FROM (now() - PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY p.last_checked_at)))::float AS p95_age_seconds
          FROM #{PARTITIONS_TABLE} p
          #{filter_sql}
        SQL
        "partition_round_trip_stats",
        params
      )
      row = result.first || {}
      {
        active_partitions:  row["active_partitions"].to_i,
        never_checked:      row["never_checked"].to_i,
        in_backoff:         row["in_backoff"].to_i,
        oldest_age_seconds: row["oldest_age_seconds"]&.to_f,
        p50_age_seconds:    row["p50_age_seconds"]&.to_f,
        p95_age_seconds:    row["p95_age_seconds"]&.to_f
      }
    end

    # Per-policy partition counts in one grouped query, keyed by
    # policy_name → { pending, partitions, paused }. Replaces calling
    # Partition.for_policy(name).sum/.count/.paused.count once per policy on
    # the policies index (3N queries → 1).
    def partition_counts_by_policy
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            policy_name,
            COALESCE(SUM(pending_count), 0)::int                 AS pending,
            COUNT(*)::int                                        AS partitions,
            COUNT(*) FILTER (WHERE status = 'paused')::int       AS paused
          FROM #{PARTITIONS_TABLE}
          GROUP BY policy_name
        SQL
        "partition_counts_by_policy",
        []
      )
      result.to_a.each_with_object({}) do |r, h|
        h[r["policy_name"]] = {
          pending:    r["pending"].to_i,
          partitions: r["partitions"].to_i,
          paused:     r["paused"].to_i
        }
      end
    end

    # Per-policy round-trip stats in one grouped query, keyed by
    # policy_name. Only the fields the dashboard overview renders
    # (in_backoff, oldest/p95 age); use partition_round_trip_stats for the
    # full single-policy breakdown. Replaces N per-policy calls on the
    # dashboard. Same percentile-inversion note as partition_round_trip_stats.
    def partition_round_trip_stats_by_policy
      result = connection.exec_query(
        <<~SQL.squish,
          SELECT
            p.policy_name,
            COUNT(*) FILTER (WHERE p.next_eligible_at IS NOT NULL AND p.next_eligible_at > now())::int AS in_backoff,
            EXTRACT(EPOCH FROM (now() - MIN(p.last_checked_at)))::float AS oldest_age_seconds,
            EXTRACT(EPOCH FROM (now() - PERCENTILE_DISC(0.05) WITHIN GROUP (ORDER BY p.last_checked_at)))::float AS p95_age_seconds
          FROM #{PARTITIONS_TABLE} p
          WHERE p.status = 'active' AND p.pending_count > 0
          GROUP BY p.policy_name
        SQL
        "partition_round_trip_stats_by_policy",
        []
      )
      result.to_a.each_with_object({}) do |r, h|
        h[r["policy_name"]] = {
          in_backoff:         r["in_backoff"].to_i,
          oldest_age_seconds: r["oldest_age_seconds"]&.to_f,
          p95_age_seconds:    r["p95_age_seconds"]&.to_f
        }
      end
    end

    # ----- adaptive_concurrency stats -----------------------------------------

    # Insert a fresh stats row for the given partition if none exists.
    # Idempotent — runs as `INSERT … ON CONFLICT DO NOTHING`. Cheap to
    # call on every admission so the gate's evaluate path can read
    # current_max safely without checking for existence first.
    def adaptive_seed!(policy_name:, partition_key:, initial_max:)
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{ADAPTIVE_TABLE}
            (policy_name, partition_key, current_max, ewma_latency_ms,
             sample_count, created_at, updated_at)
          VALUES ($1, $2, $3, 0, 0, now(), now())
          ON CONFLICT (policy_name, partition_key) DO NOTHING
        SQL
        "adaptive_seed",
        [policy_name, partition_key, initial_max.to_i]
      )
    end

    # Fetch the AIMD-tuned cap for a partition. Returns nil when the
    # row doesn't exist yet — caller should fall back to initial_max.
    def adaptive_current_max(policy_name:, partition_key:)
      result = connection.exec_query(
        "SELECT current_max FROM #{ADAPTIVE_TABLE} WHERE policy_name = $1 AND partition_key = $2 LIMIT 1",
        "adaptive_current_max",
        [policy_name, partition_key]
      )
      row = result.first
      row && row["current_max"].to_i
    end

    # Single-statement EWMA + AIMD update. Concurrent workers can call
    # this in any order without read-modify-write races: every clause
    # reads the row's current value at the start of the UPDATE.
    #
    # ewma_latency_ms_new = ewma_latency_ms * (1 - α) + α * queue_lag_ms
    # current_max_new     = LEAST(max, GREATEST(min,
    #                         FAILED?         FLOOR(current_max * fail_factor)
    #                         OVERLOADED?     FLOOR(current_max * slow_factor)
    #                         else            current_max + 1))
    #
    # The LEAST is what stops the additive increase from running away:
    # growth is +1 per healthy perform whether or not the cap is the
    # binding constraint, so without it a partition on a slow, healthy
    # trickle climbs indefinitely — the gate quietly stops limiting, and
    # the integer column eventually overflows.
    def adaptive_record!(policy_name:, partition_key:, queue_lag_ms:, succeeded:,
                         alpha:, target_lag_ms:, fail_factor:, slow_factor:, min:, max:)
      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{ADAPTIVE_TABLE}
          SET
            ewma_latency_ms = ewma_latency_ms * (1 - $3::double precision)
                              + $3::double precision * $4::double precision,
            sample_count    = sample_count + 1,
            current_max     = LEAST($10::int, GREATEST($5::int, CASE
              WHEN $6::boolean = FALSE
                THEN FLOOR(current_max * $7::double precision)::int
              WHEN (ewma_latency_ms * (1 - $3::double precision)
                    + $3::double precision * $4::double precision) > $8::double precision
                THEN FLOOR(current_max * $9::double precision)::int
              ELSE current_max + 1
            END)),
            last_observed_at = now(),
            updated_at       = now()
          WHERE policy_name = $1 AND partition_key = $2
        SQL
        "adaptive_record",
        [policy_name, partition_key, alpha.to_f, queue_lag_ms.to_f,
         min.to_i, succeeded ? true : false,
         fail_factor.to_f, target_lag_ms.to_f, slow_factor.to_f, max.to_i]
      )
    end

    # ----- tick samples sweep -------------------------------------------------

    def sweep_old_tick_samples!(cutoff_seconds:)
      connection.exec_query(
        "DELETE FROM #{SAMPLES_TABLE} WHERE sampled_at < now() - ($1 || ' seconds')::interval",
        "sweep_old_tick_samples",
        [cutoff_seconds.to_i]
      )
    end

    # ----------------------------------------------------------------------------

    # `policy_name` sweeps one policy (with its own cutoff — see
    # TickLoop.sweep!, which gives a throttled policy a cutoff at least as
    # long as its refill window). `except_policies` is the complement: one
    # pass at the default cutoff for every partition whose policy isn't
    # registered in this process, so rows left behind by a deleted policy
    # are still collected.
    #
    # `status` is deliberately not filtered. It used to require 'active',
    # which meant a paused policy's empty partitions were never collected
    # at all — pausing is when partitions are MOST likely to go empty and
    # stay that way. Nothing is lost by collecting one: the pause flag
    # lives in dispatch_policy_policy_settings, so it still applies when
    # the partition reappears.
    # `full_bucket` — {capacity:, cutoff_seconds:} — collects a partition
    # early when its token bucket is already at capacity. Giving a
    # throttled policy a cutoff as long as its window (so the bucket is
    # never reset mid-window) otherwise means holding every partition of
    # a `per: 7.days` policy for a week, which is a lot of rows to keep
    # for state that is only interesting while it is BELOW capacity. A
    # full bucket is worth nothing: a partition that reappears starts
    # full anyway. Only available when the rate is a fixed number —
    # capacity is unknowable here otherwise.
    def sweep_inactive_partitions!(cutoff_seconds:, policy_name: nil, except_policies: [],
                                   full_bucket: nil)
      params = [cutoff_seconds.to_i]
      filter = ""
      if policy_name
        params << policy_name
        filter = "AND policy_name = $#{params.size}"
      elsif except_policies.any?
        placeholders = except_policies.map do |name|
          params << name
          "$#{params.size}"
        end
        filter = "AND policy_name NOT IN (#{placeholders.join(', ')})"
      end

      if full_bucket
        cap_idx    = params.size + 1
        cutoff_idx = params.size + 2
        params << full_bucket.fetch(:capacity).to_f
        params << full_bucket.fetch(:cutoff_seconds).to_i
        # A row with no bucket recorded at all has nothing to lose, hence
        # the COALESCE to capacity.
        full_bucket_sql = <<~SQL.squish
          OR (
            COALESCE(last_admit_at, created_at) < now() - ($#{cutoff_idx} || ' seconds')::interval
            AND COALESCE((gate_state -> 'throttle' ->> 'tokens')::double precision,
                         $#{cap_idx}::double precision) >= $#{cap_idx}::double precision
          )
        SQL
      else
        full_bucket_sql = ""
      end

      connection.exec_query(
        <<~SQL.squish,
          DELETE FROM #{PARTITIONS_TABLE}
          WHERE pending_count = 0
            #{filter}
            AND (
              COALESCE(last_admit_at, created_at) < now() - ($1 || ' seconds')::interval
              #{full_bucket_sql}
            )
        SQL
        "sweep_inactive_partitions",
        params
      )
    end

    # ----- helpers --------------------------------------------------------------

    def normalize_partition(row)
      out = {}
      row.each { |k, v| out[k.to_s] = v }
      out["context"]    = parse_jsonb(out["context"])
      out["gate_state"] = parse_jsonb(out["gate_state"])
      out
    end

    def normalize_staged(row)
      out = {}
      row.each { |k, v| out[k.to_s] = v }
      out["job_data"] = parse_jsonb(out["job_data"])
      out["context"]  = parse_jsonb(out["context"])
      out
    end

    def parse_jsonb(value)
      case value
      when Hash, Array then value
      when nil, ""      then {}
      else
        begin
          JSON.parse(value)
        rescue JSON::ParserError
          {}
        end
      end
    end

    def sample_filter(policy_name, since)
      params = [since]
      if policy_name
        params << policy_name
        ["WHERE sampled_at >= $1 AND policy_name = $2", params]
      else
        ["WHERE sampled_at >= $1", params]
      end
    end

    def next_eligible_clause(retry_after)
      if retry_after.nil?
        ["NULL", []]
      else
        # 5th param ($5) — caller appends params to those of the parent UPDATE
        ["now() + ($5 || ' seconds')::interval", [retry_after.to_f.round(3)]]
      end
    end

    # ----- role routing ---------------------------------------------------------
    #
    # Every public Repository method must run against config.database_role
    # so multi-DB setups (e.g. solid_queue on a separate :queue DB, with
    # the gem tables living there) hit the DB the staging/admission/inflight
    # state actually lives in. Otherwise staging writes the primary DB while
    # the tick reads the queue DB — silent job loss — and the concurrency
    # gate counts inflight rows in a different DB than the tracker writes.
    #
    # Rather than wrap ~25 method bodies by hand — and risk missing one as
    # the API grows — we redefine each public SQL method to run inside
    # `with_connection`. We capture the ORIGINAL as a bound closure and call
    # it directly (no `super`, no prepended module): this is immune to the
    # file being evaluated more than once in a process (dev reloader,
    # integration suites that boot the dummy app under multiple require
    # paths). Each evaluation re-wraps the freshly (re)defined originals
    # exactly once, so wrappers never stack. `connected_to(role:)` nesting
    # with the SAME role is a no-op, so the explicit `with_connection` blocks
    # at the transaction boundaries (Tick, ManualAdmission) stay correct: the
    # admission TX still opens entirely within one role context, preserving
    # the shared-connection atomicity invariant. The `connection` accessor
    # and the pure helpers are excluded — they issue no SQL of their own and
    # always run inside an already-routed caller, so wrapping them would only
    # add redundant role swaps in hot per-row loops (normalize_*/parse_jsonb
    # run once per claimed row).
    ROLE_ROUTING_EXCLUDED = %i[
      connection with_connection
      normalize_partition normalize_staged parse_jsonb
      sample_filter next_eligible_clause trend_direction
    ].freeze

    (singleton_methods(false) - ROLE_ROUTING_EXCLUDED).each do |method_name|
      original = singleton_class.instance_method(method_name)
      define_singleton_method(method_name) do |*args, **kwargs, &block|
        with_connection { original.bind_call(self, *args, **kwargs, &block) }
      end
    end
  end
end
