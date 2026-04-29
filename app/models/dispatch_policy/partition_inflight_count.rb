# frozen_string_literal: true

module DispatchPolicy
  class PartitionInflightCount < ApplicationRecord
    self.table_name = "dispatch_policy_partition_counts"

    def self.fetch_many(policy_name:, gate_name:, partition_keys:)
      return {} if partition_keys.empty?

      where(policy_name: policy_name, gate_name: gate_name.to_s, partition_key: partition_keys)
        .pluck(:partition_key, :in_flight).to_h
        .tap { |h| partition_keys.each { |k| h[k] ||= 0 } }
    end

    def self.total_for(policy_name:, gate_name:)
      where(policy_name: policy_name, gate_name: gate_name.to_s).sum(:in_flight)
    end

    def self.increment(policy_name:, gate_name:, partition_key:, by: 1)
      now = Time.current
      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, gate_name, partition_key, in_flight, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (policy_name, gate_name, partition_key)
        DO UPDATE SET
          in_flight  = #{quoted_table_name}.in_flight + EXCLUDED.in_flight,
          updated_at = EXCLUDED.updated_at
      SQL
      connection.exec_update(
        sanitize_sql_array([ sql, policy_name, gate_name.to_s, partition_key.to_s, by, now, now ])
      )
    end

    def self.decrement(policy_name:, gate_name:, partition_key:, by: 1)
      where(policy_name: policy_name, gate_name: gate_name.to_s, partition_key: partition_key.to_s)
        .update_all([
          "in_flight = GREATEST(in_flight - ?, 0), updated_at = ?", by, Time.current
        ])
    end

    # Bulk variant of increment for the tick path. Takes a hash of
    # {[policy_name, gate_name, partition_key] => delta} and applies
    # them all in a single INSERT … ON CONFLICT, replacing what was
    # previously N separate ON CONFLICT round-trips.
    def self.increment_many!(deltas:)
      return 0 if deltas.empty?

      now = Time.current
      values_clause = ([ "(?, ?, ?, ?::int, ?, ?)" ] * deltas.size).join(", ")
      values_args   = deltas.flat_map do |(policy_name, gate, key), delta|
        [ policy_name, gate.to_s, key.to_s, delta, now, now ]
      end

      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, gate_name, partition_key, in_flight, created_at, updated_at)
        VALUES #{values_clause}
        ON CONFLICT (policy_name, gate_name, partition_key)
        DO UPDATE SET
          in_flight  = #{quoted_table_name}.in_flight + EXCLUDED.in_flight,
          updated_at = EXCLUDED.updated_at
      SQL

      connection.exec_update(sanitize_sql_array([ sql, *values_args ]))
      connection.clear_query_cache
    end
  end
end
