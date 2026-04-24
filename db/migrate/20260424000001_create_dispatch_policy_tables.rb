# frozen_string_literal: true

class CreateDispatchPolicyTables < ActiveRecord::Migration[7.1]
  def change
    create_table :dispatch_policy_staged_jobs do |t|
      t.string   :job_class,       null: false
      t.string   :policy_name,     null: false
      t.jsonb    :arguments,       null: false
      t.jsonb    :snapshot,        null: false, default: {}
      t.jsonb    :context,         null: false, default: {}
      t.integer  :priority,        null: false, default: 100
      t.datetime :not_before_at
      t.datetime :staged_at,       null: false
      t.datetime :admitted_at
      t.datetime :completed_at
      t.datetime :lease_expires_at
      t.string   :active_job_id
      t.string   :dedupe_key
      t.string   :round_robin_key
      t.jsonb    :partitions,      null: false, default: {}

      t.timestamps
    end

    add_index :dispatch_policy_staged_jobs,
      %i[policy_name priority staged_at],
      where: "admitted_at IS NULL",
      name: "idx_dp_staged_dispatch_order"

    add_index :dispatch_policy_staged_jobs,
      %i[policy_name dedupe_key],
      unique: true,
      where: "dedupe_key IS NOT NULL AND completed_at IS NULL",
      name: "idx_dp_staged_dedupe_active"

    add_index :dispatch_policy_staged_jobs,
      %i[lease_expires_at],
      where: "admitted_at IS NOT NULL",
      name: "idx_dp_staged_lease_expires"

    add_index :dispatch_policy_staged_jobs,
      %i[completed_at],
      where: "completed_at IS NOT NULL",
      name: "idx_dp_staged_completed_at"

    add_index :dispatch_policy_staged_jobs,
      %i[policy_name round_robin_key priority staged_at],
      where: "admitted_at IS NULL AND round_robin_key IS NOT NULL",
      name: "idx_dp_staged_round_robin"

    create_table :dispatch_policy_partition_counts do |t|
      t.string  :policy_name,   null: false
      t.string  :gate_name,     null: false
      t.string  :partition_key, null: false, default: "default"
      t.integer :in_flight,     null: false, default: 0

      t.timestamps
    end

    add_index :dispatch_policy_partition_counts,
      %i[policy_name gate_name partition_key],
      unique: true,
      name: "idx_dp_partition_counts_unique"

    create_table :dispatch_policy_throttle_buckets do |t|
      t.string   :policy_name,   null: false
      t.string   :gate_name,     null: false
      t.string   :partition_key, null: false, default: "default"
      t.float    :tokens,        null: false
      t.datetime :refilled_at,   null: false

      t.timestamps
    end

    add_index :dispatch_policy_throttle_buckets,
      %i[policy_name gate_name partition_key],
      unique: true,
      name: "idx_dp_throttle_buckets_unique"
  end
end
