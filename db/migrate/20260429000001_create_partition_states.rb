# frozen_string_literal: true

class CreatePartitionStates < ActiveRecord::Migration[7.1]
  def change
    # Per-(policy, partition_key) record of when a partition was last
    # admitted. Drives the round-robin LRU cursor in fetch_*_batch so
    # high-cardinality policies don't starve partitions that sort late
    # in the round_robin_key index. Independent of which gates a policy
    # uses — the existing partition_counts only has rows for gates with
    # tracks_inflight?, which excluded throttle-only policies.
    create_table :dispatch_policy_partition_states do |t|
      t.string   :policy_name,      null: false
      t.string   :partition_key,    null: false
      t.datetime :last_admitted_at, null: false
      t.timestamps
    end

    add_index :dispatch_policy_partition_states,
      %i[policy_name partition_key],
      unique: true,
      name: "idx_dp_partition_states_unique"

    # LRU lookup: ORDER BY last_admitted_at ASC NULLS FIRST LIMIT cap
    # — for the cursor scan in fetch_round_robin_batch.
    add_index :dispatch_policy_partition_states,
      %i[policy_name last_admitted_at],
      name: "idx_dp_partition_states_lru"
  end
end
