# frozen_string_literal: true

class AddPendingCountToPartitionStates < ActiveRecord::Migration[7.1]
  def up
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

    # Backfill: existing pending staged_jobs with a round_robin_key
    # need to be reflected in partition_states.pending_count, otherwise
    # pluck_active_partitions (which now reads from this table) won't
    # see them and they'd never be admitted via round-robin until a
    # fresh stage_many! call re-populated their partition state.
    # ON CONFLICT clause makes this safe to re-run.
    execute <<~SQL.squish
      INSERT INTO dispatch_policy_partition_states
        (policy_name, partition_key, pending_count, last_admitted_at, created_at, updated_at)
      SELECT
        policy_name,
        round_robin_key       AS partition_key,
        COUNT(*)              AS pending_count,
        NULL                  AS last_admitted_at,
        NOW()                 AS created_at,
        NOW()                 AS updated_at
      FROM dispatch_policy_staged_jobs
      WHERE admitted_at IS NULL
        AND completed_at IS NULL
        AND round_robin_key IS NOT NULL
      GROUP BY policy_name, round_robin_key
      ON CONFLICT (policy_name, partition_key)
      DO UPDATE SET
        pending_count = dispatch_policy_partition_states.pending_count + EXCLUDED.pending_count,
        updated_at    = EXCLUDED.updated_at
    SQL
  end

  def down
    remove_index :dispatch_policy_partition_states, name: "idx_dp_partition_states_active_lru"
    remove_column :dispatch_policy_partition_states, :pending_count
  end
end
