# frozen_string_literal: true

module DispatchPolicy
  # Per-(policy, partition_key) LRU record + pending-count materializer.
  #
  # Written in bulk by StagedJob.stage_many! / .stage! (increment on
  # new pending rows) and Tick.run_policy.admit_many! (decrement on
  # admission, set last_admitted_at). Lets pluck_active_partitions
  # read directly from this table with an index-seek instead of doing
  # a DISTINCT scan over staged_jobs — bounded fetch wall time
  # regardless of total partition count.
  class PartitionState < ApplicationRecord
    self.table_name = "dispatch_policy_partition_states"

    # Bulk-upsert pending_count increments. Called from staging when
    # new rows enter the pending state for one or more partitions.
    # `counts` is { partition_key => delta_to_add }.
    def self.bulk_increment_pending!(policy_name:, counts:, now: Time.current)
      return 0 if counts.empty?

      values_clause = ([ "(?, ?, ?::int, ?, ?, ?)" ] * counts.size).join(", ")
      values_args   = counts.flat_map do |partition_key, delta|
        # last_admitted_at NULL on first insert — it's the "never
        # admitted" sentinel that makes this partition sort first
        # in the LRU pluck.
        [ policy_name, partition_key.to_s, delta.to_i, nil, now, now ]
      end

      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, partition_key, pending_count, last_admitted_at, created_at, updated_at)
        VALUES #{values_clause}
        ON CONFLICT (policy_name, partition_key)
        DO UPDATE SET
          pending_count = #{quoted_table_name}.pending_count + EXCLUDED.pending_count,
          updated_at    = EXCLUDED.updated_at
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args ]))
      connection.clear_query_cache
    end

    # Bulk-update admission events: set last_admitted_at to `now`
    # AND decrement pending_count by the per-partition admit count.
    # `counts` is { partition_key => admit_count }. UPDATE-only —
    # rows must already exist (they were inserted by stage_many!).
    # GREATEST clamp prevents pending_count going negative if state
    # is out of sync (pre-migration data).
    def self.admit_many!(policy_name:, counts:, now: Time.current)
      return 0 if counts.empty?

      values_clause = ([ "(?, ?, ?::int)" ] * counts.size).join(", ")
      values_args   = counts.flat_map do |partition_key, delta|
        [ policy_name, partition_key.to_s, delta.to_i ]
      end

      sql = <<~SQL.squish
        UPDATE #{quoted_table_name} AS ps
           SET pending_count    = GREATEST(ps.pending_count - d.delta, 0),
               last_admitted_at = ?,
               updated_at       = ?
          FROM (VALUES #{values_clause}) AS d(policy_name, partition_key, delta)
         WHERE ps.policy_name   = d.policy_name
           AND ps.partition_key = d.partition_key
      SQL

      connection.exec_update(sanitize_sql_array([ sql, now, now, *values_args ]))
      connection.clear_query_cache
    end
  end
end
