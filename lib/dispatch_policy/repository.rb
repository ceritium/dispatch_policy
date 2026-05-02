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
    SAMPLES_TABLE     = "dispatch_policy_tick_samples"

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
    def stage_many!(rows)
      return 0 if rows.empty?

      connection.transaction(requires_new: true) do
        values_sql = []
        params     = []
        rows.each_with_index do |row, idx|
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

        rows.group_by { |r| [r[:policy_name], r[:partition_key]] }.each do |(policy_name, partition_key), group|
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
    def claim_staged_jobs!(policy_name:, partition_key:, limit:, gate_state_patch:, retry_after:,
                           half_life_seconds: nil)
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

      record_partition_admit!(
        policy_name:       policy_name,
        partition_key:     partition_key,
        admitted:          rows.size,
        gate_state_patch:  gate_state_patch,
        retry_after:       retry_after,
        half_life_seconds: half_life_seconds
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
    def record_partition_admit!(policy_name:, partition_key:, admitted:, gate_state_patch:,
                                retry_after:, half_life_seconds: nil)
      next_eligible_sql, next_eligible_params = next_eligible_clause(retry_after)
      gate_state_json = JSON.dump(gate_state_patch || {})

      params = [policy_name, partition_key, admitted, gate_state_json, *next_eligible_params]

      if half_life_seconds && half_life_seconds.to_f.positive?
        # decay constant τ such that exp(-Δt/τ) halves every half_life:
        # τ = half_life / ln(2). NULLIF guards a degenerate τ=0.
        decay_idx        = params.size + 1
        admitted_idx_for_ewma = 3
        decay_tau        = half_life_seconds.to_f / Math.log(2)
        params << decay_tau
        decay_sql = <<~SQL.squish
          decayed_admits     = decayed_admits *
                                exp(- COALESCE(EXTRACT(EPOCH FROM (now() - decayed_admits_at)), 0)
                                     / NULLIF($#{decay_idx}::double precision, 0))
                              + $#{admitted_idx_for_ewma},
          decayed_admits_at  = now(),
        SQL
      else
        decay_sql = ""
      end

      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{PARTITIONS_TABLE}
          SET pending_count    = GREATEST(pending_count - $3, 0),
              total_admitted   = total_admitted + $3,
              last_admit_at    = CASE WHEN $3 > 0 THEN now() ELSE last_admit_at END,
              gate_state       = gate_state || $4::jsonb,
              next_eligible_at = #{next_eligible_sql},
              #{decay_sql}
              updated_at       = now()
          WHERE policy_name = $1 AND partition_key = $2
        SQL
        "record_partition_admit",
        params
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
    # statement equivalent to N sequential UPDATEs in correctness terms;
    # the row-level locks held by `claim_partitions` (FOR UPDATE SKIP
    # LOCKED, last_checked_at bumped) keep concurrent ticks away from the
    # same partitions while we batch.
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
                WHEN v.retry_after_secs IS NULL THEN NULL
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

    def sweep_stale_inflight!(cutoff_seconds:)
      connection.exec_query(
        <<~SQL.squish,
          DELETE FROM #{INFLIGHT_TABLE}
          WHERE heartbeat_at < now() - ($1 || ' seconds')::interval
        SQL
        "sweep_stale_inflight",
        [cutoff_seconds.to_i]
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

    def sweep_old_tick_samples!(cutoff_seconds:)
      connection.exec_query(
        "DELETE FROM #{SAMPLES_TABLE} WHERE sampled_at < now() - ($1 || ' seconds')::interval",
        "sweep_old_tick_samples",
        [cutoff_seconds.to_i]
      )
    end

    # ----------------------------------------------------------------------------

    def sweep_inactive_partitions!(cutoff_seconds:)
      connection.exec_query(
        <<~SQL.squish,
          DELETE FROM #{PARTITIONS_TABLE}
          WHERE pending_count = 0
            AND status = 'active'
            AND (
              (last_admit_at IS NOT NULL AND last_admit_at < now() - ($1 || ' seconds')::interval)
              OR
              (last_admit_at IS NULL AND created_at < now() - ($1 || ' seconds')::interval)
            )
        SQL
        "sweep_inactive_partitions",
        [cutoff_seconds.to_i]
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
  end
end
