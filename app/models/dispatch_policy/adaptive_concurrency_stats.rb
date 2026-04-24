# frozen_string_literal: true

module DispatchPolicy
  class AdaptiveConcurrencyStats < ApplicationRecord
    self.table_name = "dispatch_policy_adaptive_concurrency_stats"

    # Seed a stats row if one doesn't exist yet. Mirrors ThrottleBucket.lock.
    def self.seed!(policy_name:, gate_name:, partition_key:, initial_max:)
      now = Time.current
      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, gate_name, partition_key, current_max,
           ewma_latency_ms, sample_count, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, 0, ?, ?)
        ON CONFLICT (policy_name, gate_name, partition_key) DO NOTHING
      SQL
      connection.exec_update(
        sanitize_sql_array([
          sql, policy_name, gate_name.to_s, partition_key.to_s,
          initial_max.to_i, now, now
        ])
      )
    end

    def self.fetch_many(policy_name:, gate_name:, partition_keys:)
      return {} if partition_keys.empty?
      where(policy_name: policy_name, gate_name: gate_name.to_s, partition_key: partition_keys)
        .pluck(:partition_key, :current_max, :ewma_latency_ms)
        .each_with_object({}) { |(k, c, l), h| h[k] = { current_max: c, ewma_latency_ms: l } }
    end

    # Single-statement EWMA + AIMD update so concurrent performs can't race
    # on read-modify-write. Seed first (INSERT ON CONFLICT DO NOTHING), then
    # apply the adjustment.
    def self.record_observation!(
      policy_name:, gate_name:, partition_key:,
      queue_lag_ms:, succeeded:,
      alpha:, min:, target_lag_ms:,
      fail_factor:, slow_factor:, initial_max:
    )
      seed!(
        policy_name:   policy_name,
        gate_name:     gate_name,
        partition_key: partition_key,
        initial_max:   initial_max
      )

      # Feedback signal is queue_lag (admitted_at → perform_start). When
      # the adapter queue is empty, lag ≈ 0 → +1 grow. When the queue
      # backs up, lag rises past target → multiplicative shrink. Failures
      # shrink harder. Only `min` is enforced so a partition can't lock
      # out entirely.
      sql = <<~SQL.squish
        UPDATE #{quoted_table_name}
        SET
          ewma_latency_ms = ewma_latency_ms * (1 - ?) + ? * ?,
          sample_count    = sample_count + 1,
          current_max = GREATEST(?, CASE
            WHEN ? = FALSE                                THEN FLOOR(current_max * ?)::int
            WHEN (ewma_latency_ms * (1 - ?) + ? * ?) > ?  THEN FLOOR(current_max * ?)::int
            ELSE current_max + 1
          END),
          last_observed_at = ?,
          updated_at       = ?
        WHERE policy_name = ? AND gate_name = ? AND partition_key = ?
      SQL

      now = Time.current
      connection.exec_update(
        sanitize_sql_array([
          sql,
          alpha, alpha, queue_lag_ms,
          min.to_i,
          succeeded, fail_factor,
          alpha, alpha, queue_lag_ms, target_lag_ms, slow_factor,
          now, now,
          policy_name, gate_name.to_s, partition_key.to_s
        ])
      )
    end
  end
end
