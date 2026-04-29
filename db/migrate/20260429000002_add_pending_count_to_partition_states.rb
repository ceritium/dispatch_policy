# frozen_string_literal: true

class AddPendingCountToPartitionStates < ActiveRecord::Migration[7.1]
  def change
    # Counter of staged_jobs in pending state for this (policy,
    # partition_key). Incremented on stage_many!, decremented on
    # mark_admitted_many!. Lets pluck_active_partitions skip the
    # O(N) DISTINCT scan over staged_jobs and read directly from
    # partition_states with an index seek.
    add_column :dispatch_policy_partition_states, :pending_count,
      :integer, null: false, default: 0

    # Partial index: only partitions with pending rows. ORDER BY
    # last_admitted_at ASC NULLS FIRST + LIMIT cap becomes a
    # bounded index scan regardless of how many total partitions
    # the policy has accumulated.
    add_index :dispatch_policy_partition_states,
      %i[policy_name last_admitted_at],
      where: "pending_count > 0",
      name: "idx_dp_partition_states_active_lru"
  end
end
