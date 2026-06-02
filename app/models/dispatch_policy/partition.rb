# frozen_string_literal: true

module DispatchPolicy
  class Partition < ApplicationRecord
    self.table_name = "dispatch_policy_partitions"

    scope :for_policy, ->(name) { where(policy_name: name) }
    scope :for_shard,  ->(s)    { s ? where(shard: s) : all }
    scope :active,     -> { where(status: "active") }
    scope :paused,     -> { where(status: "paused") }
    scope :pending,    -> { where("pending_count > 0") }

    def paused?
      status == "paused"
    end
  end
end
