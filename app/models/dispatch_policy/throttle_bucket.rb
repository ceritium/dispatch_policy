# frozen_string_literal: true

module DispatchPolicy
  class ThrottleBucket < ApplicationRecord
    self.table_name = "dispatch_policy_throttle_buckets"

    def self.lock(policy_name:, gate_name:, partition_key:, burst:)
      now = Time.current
      seed_sql = <<~SQL.squish
        INSERT INTO #{quoted_table_name}
          (policy_name, gate_name, partition_key, tokens, refilled_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (policy_name, gate_name, partition_key) DO NOTHING
      SQL
      connection.exec_update(
        sanitize_sql_array([
          seed_sql, policy_name, gate_name.to_s, partition_key.to_s,
          burst.to_f, now, now, now
        ])
      )

      where(policy_name: policy_name, gate_name: gate_name.to_s, partition_key: partition_key.to_s)
        .lock("FOR UPDATE")
        .first!
    end

    def refill!(rate:, per:, burst:)
      now = Time.current
      elapsed = (now - refilled_at).to_f
      new_tokens = tokens + (rate * elapsed / per)
      self.tokens = [ new_tokens, burst.to_f ].min
      self.refilled_at = now
    end

    def consume(n = 1)
      return false if tokens < n
      self.tokens -= n
      true
    end
  end
end
