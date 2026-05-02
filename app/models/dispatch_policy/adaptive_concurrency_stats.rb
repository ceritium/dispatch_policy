# frozen_string_literal: true

module DispatchPolicy
  # One row per (policy_name, partition_key) for partitions whose policy
  # declares an `:adaptive_concurrency` gate. Holds the AIMD-tuned
  # `current_max` plus the EWMA of recent queue-lag observations the cap
  # adapts on.
  #
  # Read by `Gates::AdaptiveConcurrency#evaluate` to learn how many jobs
  # this partition may admit right now. Written atomically by
  # `Repository.adaptive_record!` from `InflightTracker.track`'s ensure
  # block after each perform — the EWMA + AIMD update lives in a single
  # SQL statement so concurrent workers can't race on read-modify-write.
  class AdaptiveConcurrencyStats < ApplicationRecord
    self.table_name = "dispatch_policy_adaptive_concurrency_stats"

    scope :for_policy, ->(name) { where(policy_name: name) }
  end
end
