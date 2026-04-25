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

    # Single-statement debit used by :time_budget on perform completion.
    # No SELECT-then-UPDATE race: the subtraction lives entirely in SQL
    # so concurrent completions can't lose a debit. Tokens may go negative
    # — that's intentional (see Gates::TimeBudget).
    def self.debit_ms!(policy_name:, gate_name:, partition_key:, ms:)
      connection.exec_update(
        sanitize_sql_array([
          <<~SQL.squish, ms.to_f, Time.current, policy_name, gate_name.to_s, partition_key.to_s
            UPDATE #{quoted_table_name}
               SET tokens = tokens - ?, updated_at = ?
             WHERE policy_name = ? AND gate_name = ? AND partition_key = ?
          SQL
        ])
      )
    end
  end
end
