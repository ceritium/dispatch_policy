# frozen_string_literal: true

module DispatchPolicy
  # Minute-bucketed observability per (policy, partition). Any gate with
  # partition_by gets an observation row here — adaptive, throttle,
  # concurrency, whatever — so the admin chart shows queue lag / throughput
  # for all partitioned policies, not just the adaptive ones.
  #
  # One row per (policy, partition, minute): total_lag_ms accumulates the
  # sum of queue_lag_ms observations in that minute, total_duration_ms
  # accumulates perform durations (used by :time_budget and :fair_time_share),
  # observation_count increments, max_lag_ms / max_duration_ms track worst
  # spikes. Averages are derived on read as total / count.
  class PartitionObservation < ApplicationRecord
    self.table_name = "dispatch_policy_partition_observations"

    OBSERVATION_TTL = 2 * 60 * 60  # 2 hours

    def self.observe!(policy_name:, partition_key:, queue_lag_ms:, duration_ms: 0, current_max: nil)
      return if partition_key.nil? || partition_key.to_s.empty?

      now = Time.current
      lag = queue_lag_ms.to_i
      dur = duration_ms.to_i
      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, partition_key, minute_bucket,
           total_lag_ms, total_duration_ms, observation_count,
           max_lag_ms, max_duration_ms, current_max,
           created_at, updated_at)
        VALUES (?, ?, date_trunc('minute', ?::timestamp), ?, ?, 1, ?, ?, ?, ?, ?)
        ON CONFLICT (policy_name, partition_key, minute_bucket)
        DO UPDATE SET
          total_lag_ms      = #{quoted_table_name}.total_lag_ms + EXCLUDED.total_lag_ms,
          total_duration_ms = #{quoted_table_name}.total_duration_ms + EXCLUDED.total_duration_ms,
          observation_count = #{quoted_table_name}.observation_count + 1,
          max_lag_ms        = GREATEST(#{quoted_table_name}.max_lag_ms, EXCLUDED.max_lag_ms),
          max_duration_ms   = GREATEST(#{quoted_table_name}.max_duration_ms, EXCLUDED.max_duration_ms),
          current_max       = COALESCE(EXCLUDED.current_max, #{quoted_table_name}.current_max),
          updated_at        = EXCLUDED.updated_at
      SQL
      connection.exec_update(
        sanitize_sql_array([
          sql, policy_name, partition_key.to_s, now,
          lag, dur, lag, dur, current_max, now, now
        ])
      )
    end

    # Sum of perform durations per partition over the last `window` seconds.
    # Used by :fair_time_share to bias admission ordering toward partitions
    # that have consumed less compute time recently.
    def self.consumed_ms_by_partition(policy_name:, partition_keys:, window:)
      return {} if partition_keys.empty?

      rows = where(policy_name: policy_name, partition_key: partition_keys.map(&:to_s))
        .where("minute_bucket >= ?", Time.current - window)
        .group(:partition_key)
        .pluck(Arel.sql("partition_key, SUM(total_duration_ms), SUM(observation_count)"))
      rows.each_with_object({}) do |(key, total, count), acc|
        acc[key] = { consumed_ms: total.to_i, count: count.to_i }
      end
    end

    def self.prune!
      where("minute_bucket < ?", Time.current - OBSERVATION_TTL).delete_all
    end
  end
end
