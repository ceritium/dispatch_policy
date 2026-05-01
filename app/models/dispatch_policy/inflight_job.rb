# frozen_string_literal: true

module DispatchPolicy
  class InflightJob < ApplicationRecord
    self.table_name = "dispatch_policy_inflight_jobs"

    scope :for_partition, ->(policy_name, partition_key) {
      where(policy_name: policy_name, partition_key: partition_key)
    }
    scope :stale, ->(cutoff) { where("heartbeat_at < ?", cutoff) }
  end
end
