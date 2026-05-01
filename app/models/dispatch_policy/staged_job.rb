# frozen_string_literal: true

module DispatchPolicy
  class StagedJob < ApplicationRecord
    self.table_name = "dispatch_policy_staged_jobs"

    scope :for_policy,    ->(name) { where(policy_name: name) }
    scope :for_partition, ->(name, key) { where(policy_name: name, partition_key: key) }
    scope :due,           -> { where("scheduled_at IS NULL OR scheduled_at <= now()") }
    scope :recent,        -> { order(enqueued_at: :desc) }
  end
end
