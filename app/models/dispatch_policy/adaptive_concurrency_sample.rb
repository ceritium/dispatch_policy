# frozen_string_literal: true

module DispatchPolicy
  # Minute-bucketed snapshots of adaptive stats for charts. Written by
  # AdaptiveConcurrencyStats.record_observation! via upsert on
  # (policy, gate, partition, minute_bucket); pruned periodically.
  class AdaptiveConcurrencySample < ApplicationRecord
    self.table_name = "dispatch_policy_adaptive_concurrency_samples"

    SAMPLE_TTL = 2 * 60 * 60  # 2 hours

    def self.upsert_current!(policy_name:, gate_name:, partition_key:,
                             ewma_latency_ms:, current_max:)
      now = Time.current
      sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, gate_name, partition_key, minute_bucket,
           ewma_latency_ms, current_max, created_at, updated_at)
        VALUES (?, ?, ?, date_trunc('minute', ?::timestamp), ?, ?, ?, ?)
        ON CONFLICT (policy_name, gate_name, partition_key, minute_bucket)
        DO UPDATE SET
          ewma_latency_ms = EXCLUDED.ewma_latency_ms,
          current_max     = EXCLUDED.current_max,
          updated_at      = EXCLUDED.updated_at
      SQL
      connection.exec_update(
        sanitize_sql_array([
          sql,
          policy_name, gate_name.to_s, partition_key.to_s, now,
          ewma_latency_ms.to_f, current_max.to_i, now, now
        ])
      )
    end

    def self.prune!
      where("minute_bucket < ?", Time.current - SAMPLE_TTL).delete_all
    end
  end
end
