# frozen_string_literal: true

module DispatchPolicy
  # The dispatcher's source of truth. One row per (policy_name,
  # partition_key) holds every piece of admission state — pending
  # demand, in-flight count + concurrency cap, throttle token bucket,
  # and the `ready` flag that drives the partial index used as the
  # dispatch ready queue.
  #
  # All writes go through the bulk class methods below. Single-row
  # AR updates would be correct but would lose the index-friendly
  # composite UPDATE shape the dispatcher relies on.
  class PolicyPartition < ApplicationRecord
    self.table_name = "dispatch_policy_partitions"
    # Composite primary key — Rails 7.1 supports it natively.
    self.primary_key = %i[policy_name partition_key]

    # ─── Stage path ──────────────────────────────────────────────

    # Bulk-upsert: increments pending_count by `delta` for each
    # partition, seeding new rows with the policy's gate config.
    # `counts` is { partition_key => delta }. Idempotent on rerun.
    #
    # Sets ready = TRUE for fresh rows. For existing rows, ready is
    # left alone — we never re-enable here, only at admit/release/
    # unblock_due (the events that actually clear the blocker).
    def self.bulk_seed!(policy:, counts:, now: Time.current)
      return 0 if counts.empty?

      cmax  = policy.concurrency_max
      rate  = policy.throttle_rate
      burst = policy.throttle_burst

      values_clause = ([ "(?, ?, ?::int, ?::int, ?::numeric, ?::numeric, ?::int, ?, ?, ?)" ] * counts.size).join(", ")
      values_args   = counts.flat_map do |pk, delta|
        [
          policy.name, pk.to_s, delta.to_i,
          cmax,
          burst, # initial tokens = full burst
          rate,
          burst,
          rate ? now : nil,
          now, now
        ]
      end

      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, partition_key, pending_count, concurrency_max,
           tokens, throttle_rate, throttle_burst, refilled_at,
           created_at, updated_at)
        VALUES #{values_clause}
        ON CONFLICT (policy_name, partition_key) DO UPDATE SET
          pending_count = #{quoted_table_name}.pending_count + EXCLUDED.pending_count,
          updated_at    = EXCLUDED.updated_at
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args ]))
      connection.clear_query_cache
    end

    # ─── Dispatch path ───────────────────────────────────────────

    # Pluck up to `limit` admissible (policy_name, partition_key,
    # slots) tuples in LRU order. `slots` = how many jobs the
    # dispatcher can pull from this partition this tick, accounting
    # for concurrency headroom and effective tokens (refill computed
    # inline). Returns an Array<Hash>.
    def self.pluck_admissible(policy_name, limit:, batch_size:)
      sql = <<~SQL.squish
        SELECT partition_key,
               LEAST(
                 COALESCE(concurrency_max - in_flight, ?::int),
                 COALESCE(FLOOR(LEAST(
                   throttle_burst::numeric,
                   tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - refilled_at))) * throttle_rate
                 ))::int, ?::int),
                 ?::int
               ) AS slots
        FROM #{quoted_table_name}
        WHERE policy_name = ?
          AND pending_count > 0
          AND ready = TRUE
        ORDER BY last_admitted_at NULLS FIRST
        LIMIT ?
      SQL
      result = connection.exec_query(
        sanitize_sql_array([ sql, batch_size, batch_size, batch_size, policy_name, limit ])
      )
      # Filter out partitions where slots fell to 0 between read and
      # the LEAST clamp (e.g. throttle_rate=0 with empty bucket).
      result.to_a.map { |r| { partition_key: r["partition_key"], slots: r["slots"].to_i } }
        .reject { |r| r[:slots] <= 0 }
    end

    # Apply admission: per-partition decrement of pending_count,
    # increment of in_flight, decrement of tokens (with lazy refill),
    # update last_admitted_at, and recompute ready/blocked_until.
    # `counts` is { partition_key => admitted_count }.
    def self.bulk_admit!(policy_name:, counts:, now: Time.current)
      return 0 if counts.empty?

      values_clause = ([ "(?, ?::int)" ] * counts.size).join(", ")
      values_args   = counts.flat_map { |pk, n| [ pk.to_s, n.to_i ] }

      # Tokens after admit = MIN(burst, tokens + elapsed*rate) - delta
      # blocked_until: when the bucket needs to refill back to ≥1.
      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS p
           SET pending_count    = p.pending_count - d.delta,
               in_flight        = p.in_flight + d.delta,
               tokens           = CASE
                                    WHEN p.throttle_rate IS NOT NULL THEN
                                      LEAST(
                                        p.throttle_burst::numeric,
                                        p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                                      ) - d.delta
                                    ELSE p.tokens
                                  END,
               refilled_at      = CASE
                                    WHEN p.throttle_rate IS NOT NULL THEN NOW()
                                    ELSE p.refilled_at
                                  END,
               last_admitted_at = NOW(),
               ready            = (
                 (p.pending_count - d.delta) > 0
                 AND (p.concurrency_max IS NULL OR (p.in_flight + d.delta) < p.concurrency_max)
                 AND (
                   p.throttle_rate IS NULL OR
                   (LEAST(
                     p.throttle_burst::numeric,
                     p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                   ) - d.delta) >= 1
                 )
               ),
               blocked_until    = CASE
                                    WHEN p.throttle_rate IS NULL THEN NULL
                                    WHEN (LEAST(
                                            p.throttle_burst::numeric,
                                            p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                                          ) - d.delta) >= 1 THEN NULL
                                    ELSE NOW() + (
                                      (1 - (LEAST(
                                              p.throttle_burst::numeric,
                                              p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                                            ) - d.delta)) / p.throttle_rate
                                    ) * INTERVAL '1 second'
                                  END,
               updated_at       = NOW()
          FROM (VALUES #{values_clause}) AS d(partition_key, delta)
         WHERE p.policy_name   = ?
           AND p.partition_key = d.partition_key
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args, policy_name ]))
      connection.clear_query_cache
    end

    # ─── Release path (around_perform) ───────────────────────────

    # Decrement in_flight, recompute ready. Called once per
    # successfully-completed perform.
    def self.release!(policy_name:, partition_key:, by: 1)
      sql = <<~SQL.squish
        UPDATE #{quoted_table_name}
           SET in_flight = GREATEST(in_flight - ?, 0),
               ready    = (
                 pending_count > 0
                 AND (concurrency_max IS NULL OR (GREATEST(in_flight - ?, 0)) < concurrency_max)
                 AND (
                   throttle_rate IS NULL OR
                   LEAST(
                     throttle_burst::numeric,
                     tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - refilled_at))) * throttle_rate
                   ) >= 1
                 )
               ),
               updated_at = NOW()
         WHERE policy_name = ? AND partition_key = ?
      SQL
      connection.exec_update(sanitize_sql_array([ sql, by, by, policy_name, partition_key ]))
      connection.clear_query_cache
    end

    # ─── Unblock sweep ───────────────────────────────────────────

    # Flip ready = TRUE for partitions whose blocked_until has passed
    # (typically throttle waiting on token refill). The partial index
    # idx_dp_partitions_unblock makes this an index-bounded scan.
    def self.unblock_due!(policy_name:, now: Time.current)
      sql = <<~SQL.squish
        UPDATE #{quoted_table_name}
           SET ready         = TRUE,
               blocked_until = NULL,
               updated_at    = NOW()
         WHERE policy_name = ?
           AND ready = FALSE
           AND blocked_until IS NOT NULL
           AND blocked_until <= NOW()
           AND pending_count > 0
      SQL
      connection.exec_update(sanitize_sql_array([ sql, policy_name ]))
      connection.clear_query_cache
    end

    # ─── Reap path ───────────────────────────────────────────────

    # Bulk decrement in_flight after the reaper completes leases that
    # expired without their around_perform release running. counts
    # is { partition_key => reaped_count }.
    def self.bulk_decrement_in_flight!(policy_name:, counts:, now: Time.current)
      return 0 if counts.empty?

      values_clause = ([ "(?, ?::int)" ] * counts.size).join(", ")
      values_args   = counts.flat_map { |pk, n| [ pk.to_s, n.to_i ] }

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS p
           SET in_flight = GREATEST(p.in_flight - d.delta, 0),
               ready    = (
                 p.pending_count > 0
                 AND (p.concurrency_max IS NULL OR (GREATEST(p.in_flight - d.delta, 0)) < p.concurrency_max)
                 AND (
                   p.throttle_rate IS NULL OR
                   LEAST(
                     p.throttle_burst::numeric,
                     p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                   ) >= 1
                 )
               ),
               updated_at = NOW()
          FROM (VALUES #{values_clause}) AS d(partition_key, delta)
         WHERE p.policy_name   = ?
           AND p.partition_key = d.partition_key
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args, policy_name ]))
      connection.clear_query_cache
    end

    # ─── Maintenance ─────────────────────────────────────────────

    # Drop fully-drained rows older than `ttl`. Run periodically by
    # TickLoop boot. Cheap because of the partial index on (pending_count, in_flight, updated_at).
    def self.purge_drained!(ttl_seconds: 1_800)
      where("pending_count = 0 AND in_flight = 0 AND updated_at < ?", ttl_seconds.seconds.ago)
        .in_batches(of: 5_000) { |rel| rel.delete_all }
    end
  end
end
