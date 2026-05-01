# frozen_string_literal: true

module DispatchPolicy
  class Partition < ApplicationRecord
    self.table_name = "dispatch_policy_partitions"

    scope :for_policy, ->(name) { where(policy_name: name) }
    scope :active,     -> { where(status: "active") }
    scope :paused,     -> { where(status: "paused") }
    scope :pending,    -> { where("pending_count > 0") }
    scope :stale_inactive, ->(cutoff) {
      where("pending_count = 0 AND in_flight_count = 0")
        .where("last_admit_at < ? OR (last_admit_at IS NULL AND created_at < ?)", cutoff, cutoff)
    }

    def paused?
      status == "paused"
    end
  end
end
