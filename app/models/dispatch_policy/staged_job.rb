# frozen_string_literal: true

module DispatchPolicy
  class StagedJob < ApplicationRecord
    self.table_name = "dispatch_policy_staged_jobs"

    scope :for_policy,    ->(name) { where(policy_name: name) }
    scope :for_partition, ->(name, key) { where(policy_name: name, partition_key: key) }
    # Bound clock, not `now()`. `scheduled_at` is written by the
    # application into a `timestamp WITHOUT time zone` column, so comparing
    # it against a timestamptz reinterprets it in the session's TimeZone —
    # the A11 mismatch, and this scope is the half of it that lives outside
    # Repository. `PartitionsController#drain` counts what is left with it,
    # so on a skewed session the flash reports "3 still pending — click
    # drain again" about rows the claim cannot move, forever.
    scope :due,           -> { deliverable.where("scheduled_at IS NULL OR scheduled_at <= ?", DispatchPolicy::Repository.app_clock) }
    # Quarantined rows are not work any more: the claim skips them and
    # they have already been taken out of pending_count, so counting them
    # as pending would tell the operator a partition still has a backlog
    # that nothing is ever going to move.
    scope :deliverable,   -> { where(failed_at: nil) }
    scope :quarantined,   -> { where.not(failed_at: nil) }
    scope :recent,        -> { order(enqueued_at: :desc) }
  end
end
