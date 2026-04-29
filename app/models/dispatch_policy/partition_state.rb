# frozen_string_literal: true

module DispatchPolicy
  # Per-(policy, partition_key) LRU record. Written in bulk by
  # Tick.run_policy on every admission and read by the round-robin
  # fetch path to order partitions by least-recently-admitted, so a
  # policy with 100k+ active partitions still admits each within a
  # bounded number of ticks instead of always starting from the same
  # spot in the round_robin_key index.
  class PartitionState < ApplicationRecord
    self.table_name = "dispatch_policy_partition_states"

    # Bulk upsert one row per (policy_name, partition_key) admitted in
    # this tick. Single INSERT … ON CONFLICT DO UPDATE statement
    # regardless of batch size.
    def self.touch_many!(rows:, now: Time.current)
      return 0 if rows.empty?

      values_clause = ([ "(?, ?, ?, ?, ?)" ] * rows.size).join(", ")
      values_args   = rows.flat_map do |policy_name, partition_key|
        [ policy_name, partition_key.to_s, now, now, now ]
      end

      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, partition_key, last_admitted_at, created_at, updated_at)
        VALUES #{values_clause}
        ON CONFLICT (policy_name, partition_key)
        DO UPDATE SET
          last_admitted_at = EXCLUDED.last_admitted_at,
          updated_at       = EXCLUDED.updated_at
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args ]))
      connection.clear_query_cache
    end
  end
end
