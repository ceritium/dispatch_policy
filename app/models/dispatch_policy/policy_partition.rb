# frozen_string_literal: true

module DispatchPolicy
  # Per-(policy, partition) row that holds demand + gate state + the
  # LRU cursor. The dispatcher pulls up to batch_size of these per
  # tick, ordered by `last_checked_at ASC NULLS FIRST`, evaluates
  # gates inline, admits whatever fits, and bumps last_checked_at on
  # every visited row — admitted or blocked. Result: every partition
  # is re-checked every ceil(N/batch_size) ticks regardless of state,
  # which is the property that lets us scale to millions of partitions
  # with bounded per-tick work.
  class PolicyPartition < ApplicationRecord
    self.table_name = "dispatch_policy_partitions"
    self.primary_key = %i[policy_name partition_key]

    # ─── Stage path ──────────────────────────────────────────────

    # Bulk-upsert: increments pending_count by `delta` for each
    # partition, seeding new rows with the policy's gate config.
    # `counts` is { partition_key => delta }. Idempotent on rerun.
    # `seeds` is an Array<Hash> with keys :partition_key, :delta and
    # an optional :concurrency_max (overriding the policy default).
    # Each entry seeds (or upserts) one (policy, partition_key) row.
    # On conflict only pending_count grows — concurrency_max is set
    # by the first job that creates the partition.
    def self.bulk_seed!(policy:, seeds:, now: Time.current)
      return 0 if seeds.empty?

      rate  = policy.throttle_rate
      burst = policy.throttle_burst
      default_cmax = policy.concurrency_max if policy.concurrency_max.is_a?(Integer)

      values_clause = ([ "(?, ?, ?::int, ?::int, ?::numeric, ?::numeric, ?::int, ?, ?, ?)" ] * seeds.size).join(", ")
      values_args   = seeds.flat_map do |seed|
        cmax = seed[:concurrency_max] || default_cmax
        [
          policy.name, seed[:partition_key].to_s, seed[:delta].to_i,
          cmax,
          burst, rate, burst,    # tokens / rate / burst
          rate ? now : nil,      # refilled_at only when throttled
          now, now               # created_at, updated_at
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

    # Pluck the next `limit` partitions in cursor order. Returns the
    # full state needed to compute admissibility in Ruby — caller
    # decides slot count per partition based on gate state and the
    # policy's batch_size.
    def self.pluck_cursor(policy_name, limit:)
      sql = <<~SQL.squish
        SELECT partition_key, pending_count, in_flight, concurrency_max,
               tokens, throttle_rate, throttle_burst, refilled_at,
               last_checked_at
        FROM #{quoted_table_name}
        WHERE policy_name = ?
          AND pending_count > 0
        ORDER BY last_checked_at NULLS FIRST
        LIMIT ?
      SQL
      connection.exec_query(sanitize_sql_array([ sql, policy_name, limit ])).to_a
    end

    # Compute slot capacity for one cursor row. Returns 0 when the
    # gates block this partition; otherwise the largest count the
    # gates would let through this tick (capped at batch_size since
    # admitting more than the global batch is pointless).
    # Cap slots at pending_count too — we'd never fetch more than
    # the partition has, and inflating slots would let a hot
    # partition swallow the global remaining budget.
    def self.slots_for(row, batch_size:, now: Time.current)
      slots = [ batch_size, row["pending_count"].to_i ].min
      if row["concurrency_max"]
        headroom = row["concurrency_max"].to_i - row["in_flight"].to_i
        slots = [ slots, headroom ].min
      end
      if row["throttle_rate"]
        elapsed = [ now - row["refilled_at"], 0 ].max
        effective = [ row["throttle_burst"].to_f,
                      row["tokens"].to_f + elapsed * row["throttle_rate"].to_f ].min
        slots = [ slots, effective.floor ].min
      end
      [ slots, 0 ].max
    end

    # Bulk update for ALL visited partitions in one tick. `visits`
    # is { partition_key => admitted_count }, where admitted_count
    # may be 0 for partitions we touched but couldn't admit from.
    # Either way last_checked_at advances to NOW() so the cursor
    # rotates past them next tick.
    def self.bulk_visit!(policy_name:, visits:, now: Time.current)
      return 0 if visits.empty?

      values_clause = ([ "(?, ?::int)" ] * visits.size).join(", ")
      values_args   = visits.flat_map { |pk, n| [ pk.to_s, n.to_i ] }

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS p
           SET pending_count   = p.pending_count - d.delta,
               in_flight       = p.in_flight + d.delta,
               tokens          = CASE
                                   WHEN p.throttle_rate IS NOT NULL THEN
                                     LEAST(
                                       p.throttle_burst::numeric,
                                       p.tokens + GREATEST(0, EXTRACT(EPOCH FROM (NOW() - p.refilled_at))) * p.throttle_rate
                                     ) - d.delta
                                   ELSE p.tokens
                                 END,
               refilled_at     = CASE
                                   WHEN p.throttle_rate IS NOT NULL THEN NOW()
                                   ELSE p.refilled_at
                                 END,
               last_checked_at = NOW(),
               updated_at      = NOW()
          FROM (VALUES #{values_clause}) AS d(partition_key, delta)
         WHERE p.policy_name   = ?
           AND p.partition_key = d.partition_key
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args, policy_name ]))
      connection.clear_query_cache
    end

    # ─── Release path (around_perform) ───────────────────────────

    # Decrement in_flight after a successful perform. Doesn't touch
    # last_checked_at — release isn't a "check" event; the cursor
    # will naturally re-evaluate this partition on its next turn,
    # and the recomputed slots will reflect the new headroom.
    def self.release!(policy_name:, partition_key:, by: 1)
      sql = <<~SQL.squish
        UPDATE #{quoted_table_name}
           SET in_flight  = GREATEST(in_flight - ?, 0),
               updated_at = NOW()
         WHERE policy_name = ? AND partition_key = ?
      SQL
      connection.exec_update(sanitize_sql_array([ sql, by, policy_name, partition_key ]))
      connection.clear_query_cache
    end

    # ─── Reap path ───────────────────────────────────────────────

    def self.bulk_decrement_in_flight!(policy_name:, counts:, now: Time.current)
      return 0 if counts.empty?

      values_clause = ([ "(?, ?::int)" ] * counts.size).join(", ")
      values_args   = counts.flat_map { |pk, n| [ pk.to_s, n.to_i ] }

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS p
           SET in_flight  = GREATEST(p.in_flight - d.delta, 0),
               updated_at = NOW()
          FROM (VALUES #{values_clause}) AS d(partition_key, delta)
         WHERE p.policy_name   = ?
           AND p.partition_key = d.partition_key
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args, policy_name ]))
      connection.clear_query_cache
    end

    # ─── Maintenance ─────────────────────────────────────────────

    def self.purge_drained!(ttl_seconds: 1_800)
      where("pending_count = 0 AND in_flight = 0 AND updated_at < ?", ttl_seconds.seconds.ago)
        .in_batches(of: 5_000) { |rel| rel.delete_all }
    end
  end
end
