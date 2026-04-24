# frozen_string_literal: true

module DispatchPolicy
  # Minute-bucketed observability per (policy, partition). Any gate with
  # partition_by gets an observation row here — adaptive, throttle,
  # concurrency, whatever — so the admin chart shows queue lag / throughput
  # for all partitioned policies, not just the adaptive ones.
  #
  # One row per (policy, partition, minute): total_lag_ms accumulates the
  # sum of queue_lag_ms observations in that minute, observation_count
  # increments, max_lag_ms tracks the worst spike. Average lag for the
  # bucket is derived on read as total / count.
  class PartitionObservation < ApplicationRecord
    self.table_name = "dispatch_policy_partition_observations"

    OBSERVATION_TTL = 2 * 60 * 60  # 2 hours

    def self.observe!(policy_name:, partition_key:, queue_lag_ms:, current_max: nil)
      return if partition_key.nil? || partition_key.to_s.empty?

      now = Time.current
      lag = queue_lag_ms.to_i
      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, partition_key, minute_bucket,
           total_lag_ms, observation_count, max_lag_ms, current_max,
           created_at, updated_at)
        VALUES (?, ?, date_trunc('minute', ?::timestamp), ?, 1, ?, ?, ?, ?)
        ON CONFLICT (policy_name, partition_key, minute_bucket)
        DO UPDATE SET
          total_lag_ms      = #{quoted_table_name}.total_lag_ms + EXCLUDED.total_lag_ms,
          observation_count = #{quoted_table_name}.observation_count + 1,
          max_lag_ms        = GREATEST(#{quoted_table_name}.max_lag_ms, EXCLUDED.max_lag_ms),
          current_max       = COALESCE(EXCLUDED.current_max, #{quoted_table_name}.current_max),
          updated_at        = EXCLUDED.updated_at
      SQL
      connection.exec_update(
        sanitize_sql_array([
          sql, policy_name, partition_key.to_s, now,
          lag, lag, current_max, now, now
        ])
      )
    end

    def self.prune!
      where("minute_bucket < ?", Time.current - OBSERVATION_TTL).delete_all
    end
  end
end
