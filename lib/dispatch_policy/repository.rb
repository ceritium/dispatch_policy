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

    module_function

    def connection
      ActiveRecord::Base.connection
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
    def stage!(policy_name:, partition_key:, queue_name:, job_class:, job_data:, context:, scheduled_at: nil, priority: 0)
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
            context:       group.last[:context] || {},
            delta_pending: group.size
          )
        end
      end
      rows.size
    end

    def upsert_partition!(policy_name:, partition_key:, queue_name:, context:, delta_pending:)
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{PARTITIONS_TABLE}
            (policy_name, partition_key, queue_name, context, context_updated_at,
             pending_count, last_enqueued_at, status, gate_state, created_at, updated_at)
          VALUES ($1, $2, $3, $4::jsonb, now(), $5, now(), 'active', '{}'::jsonb, now(), now())
          ON CONFLICT (policy_name, partition_key) DO UPDATE SET
            context             = EXCLUDED.context,
            context_updated_at  = EXCLUDED.context_updated_at,
            queue_name          = COALESCE(EXCLUDED.queue_name, #{PARTITIONS_TABLE}.queue_name),
            pending_count       = #{PARTITIONS_TABLE}.pending_count + EXCLUDED.pending_count,
            last_enqueued_at    = EXCLUDED.last_enqueued_at,
            updated_at          = now()
        SQL
        "upsert_partition",
        [policy_name, partition_key, queue_name, JSON.dump(context), delta_pending]
      )
    end

    # ----- tick path -----------------------------------------------------------

    # Lock + return up to `limit` partitions ready to be evaluated by the tick.
    # Each row's last_checked_at is bumped to now() so the next tick fairly
    # picks others. Locked rows are released when the transaction commits.
    def claim_partitions(policy_name:, limit:)
      sql = <<~SQL.squish
        WITH candidates AS (
          SELECT id FROM #{PARTITIONS_TABLE}
          WHERE policy_name = $1
            AND status = 'active'
            AND pending_count > 0
            AND (next_eligible_at IS NULL OR next_eligible_at <= now())
          ORDER BY last_checked_at NULLS FIRST, id
          LIMIT $2
          FOR UPDATE SKIP LOCKED
        )
        UPDATE #{PARTITIONS_TABLE} p
        SET last_checked_at = now()
        FROM candidates
        WHERE p.id = candidates.id
        RETURNING p.*
      SQL
      result = connection.exec_query(sql, "claim_partitions", [policy_name, limit])
      result.to_a.map { |row| normalize_partition(row) }
    end

    # Atomically claim up to `limit` staged rows for a partition (DELETE …
    # RETURNING) and update the partition's counters / gate_state /
    # next_eligible_at in the same transaction.
    #
    # When `limit <= 0` we do NOT claim rows but we DO persist the gate_state
    # patch and next_eligible_at — this is critical when a gate denies
    # admission (e.g. concurrency-full): without this, next_eligible_at would
    # stay NULL and the next tick would re-evaluate immediately, hammering
    # `count_inflight` every iteration.
    def claim_staged_jobs!(policy_name:, partition_key:, limit:, gate_state_patch:, retry_after:)
      rows =
        if limit.positive?
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
          connection.exec_query(sql_select, "claim_staged_jobs", [policy_name, partition_key, limit]).to_a
        else
          []
        end

      record_partition_evaluation!(
        policy_name:      policy_name,
        partition_key:    partition_key,
        admitted:         rows.size,
        gate_state_patch: gate_state_patch,
        retry_after:      retry_after
      )

      rows.map { |r| normalize_staged(r) }
    end

    # Persist the result of evaluating gates for a partition: decrement
    # pending_count, merge the gate_state_patch, and set next_eligible_at
    # from retry_after. Always called after claim_staged_jobs! whether or
    # not any rows were admitted.
    def record_partition_evaluation!(policy_name:, partition_key:, admitted:, gate_state_patch:, retry_after:)
      next_eligible_sql, next_eligible_params = next_eligible_clause(retry_after)
      gate_state_json = JSON.dump(gate_state_patch || {})

      connection.exec_query(
        <<~SQL.squish,
          UPDATE #{PARTITIONS_TABLE}
          SET pending_count    = GREATEST(pending_count - $3, 0),
              last_admit_at    = CASE WHEN $3 > 0 THEN now() ELSE last_admit_at END,
              gate_state       = gate_state || $4::jsonb,
              next_eligible_at = #{next_eligible_sql},
              updated_at       = now()
          WHERE policy_name = $1 AND partition_key = $2
        SQL
        "record_partition_evaluation",
        [policy_name, partition_key, admitted, gate_state_json, *next_eligible_params]
      )
    end

    # Reinsert staged rows after a failed forward (compensation). The original
    # `enqueued_at` is preserved so re-claimed jobs keep their FIFO position.
    def unclaim!(rows)
      return if rows.empty?

      values_sql = []
      params     = []
      rows.each_with_index do |row, idx|
        base = idx * 9
        values_sql << "($#{base + 1}, $#{base + 2}, $#{base + 3}, $#{base + 4}, $#{base + 5}::jsonb, $#{base + 6}::jsonb, $#{base + 7}, $#{base + 8}, COALESCE($#{base + 9}, now()))"
        params.push(
          row["policy_name"],
          row["partition_key"],
          row["queue_name"],
          row["job_class"],
          JSON.dump(row["job_data"]),
          JSON.dump(row["context"] || {}),
          row["scheduled_at"],
          row["priority"] || 0,
          row["enqueued_at"]
        )
      end
      connection.exec_query(
        <<~SQL.squish,
          INSERT INTO #{STAGED_TABLE}
            (policy_name, partition_key, queue_name, job_class, job_data, context, scheduled_at, priority, enqueued_at)
          VALUES #{values_sql.join(", ")}
        SQL
        "unclaim",
        params
      )
      grouped = rows.group_by { |r| [r["policy_name"], r["partition_key"]] }
      grouped.each do |(policy_name, partition_key), group|
        connection.exec_query(
          <<~SQL.squish,
            UPDATE #{PARTITIONS_TABLE}
            SET pending_count = pending_count + $3,
                updated_at    = now()
            WHERE policy_name = $1 AND partition_key = $2
          SQL
          "unclaim_bump_partition",
          [policy_name, partition_key, group.size]
        )
      end
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
