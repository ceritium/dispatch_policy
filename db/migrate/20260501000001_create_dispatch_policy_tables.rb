# frozen_string_literal: true

class CreateDispatchPolicyTables < ActiveRecord::Migration[7.1]
  def change
    create_table :dispatch_policy_staged_jobs do |t|
      t.string  :policy_name,   null: false
      t.string  :partition_key, null: false
      t.string  :queue_name
      t.string  :job_class,     null: false
      t.jsonb   :job_data,      null: false
      t.datetime :scheduled_at
      t.integer :priority,      default: 0, null: false
      t.datetime :enqueued_at,  null: false, default: -> { "now()" }
      t.jsonb   :context,       null: false, default: {}
    end
    add_index :dispatch_policy_staged_jobs,
              [:policy_name, :partition_key, :scheduled_at, :id],
              name: "idx_dp_staged_admission",
              order: { scheduled_at: "ASC NULLS FIRST", id: :asc }
    add_index :dispatch_policy_staged_jobs, :enqueued_at,
              name: "idx_dp_staged_enqueued_at"

    create_table :dispatch_policy_partitions do |t|
      t.string   :policy_name,        null: false
      t.string   :partition_key,      null: false
      t.string   :queue_name
      t.string   :status,             null: false, default: "active"
      t.integer  :pending_count,      null: false, default: 0
      t.bigint   :total_admitted,     null: false, default: 0
      t.jsonb    :context,            null: false, default: {}
      t.datetime :context_updated_at
      t.datetime :last_enqueued_at
      t.datetime :last_checked_at
      t.datetime :last_admit_at
      t.datetime :next_eligible_at
      t.jsonb    :gate_state,         null: false, default: {}
      t.timestamps
    end
    add_index :dispatch_policy_partitions,
              [:policy_name, :partition_key],
              unique: true,
              name:   "idx_dp_partitions_lookup"
    add_index :dispatch_policy_partitions,
              [:policy_name, :status, :next_eligible_at, :last_checked_at],
              name:  "idx_dp_partitions_tick_order",
              order: { next_eligible_at: "ASC NULLS FIRST", last_checked_at: "ASC NULLS FIRST" }

    create_table :dispatch_policy_inflight_jobs do |t|
      t.string   :policy_name,    null: false
      t.string   :partition_key,  null: false
      t.string   :active_job_id,  null: false
      t.datetime :admitted_at,    null: false, default: -> { "now()" }
      t.datetime :heartbeat_at,   null: false, default: -> { "now()" }
    end
    add_index :dispatch_policy_inflight_jobs, :active_job_id, unique: true,
              name: "idx_dp_inflight_active_job_id"
    add_index :dispatch_policy_inflight_jobs, [:policy_name, :partition_key],
              name: "idx_dp_inflight_partition"
    add_index :dispatch_policy_inflight_jobs, :heartbeat_at,
              name: "idx_dp_inflight_heartbeat"

    create_table :dispatch_policy_tick_samples do |t|
      t.string   :policy_name,        null: false
      t.datetime :sampled_at,         null: false, default: -> { "now()" }
      t.integer  :duration_ms,        null: false, default: 0
      t.integer  :partitions_seen,    null: false, default: 0
      t.integer  :partitions_admitted, null: false, default: 0
      t.integer  :partitions_denied,  null: false, default: 0
      t.integer  :jobs_admitted,      null: false, default: 0
      t.integer  :forward_failures,   null: false, default: 0
      t.integer  :pending_total,      null: false, default: 0
      t.integer  :inflight_total,     null: false, default: 0
      t.jsonb    :denied_reasons,     null: false, default: {}
    end
    add_index :dispatch_policy_tick_samples, [:policy_name, :sampled_at],
              name: "idx_dp_tick_samples_lookup",
              order: { sampled_at: :desc }
    add_index :dispatch_policy_tick_samples, :sampled_at,
              name: "idx_dp_tick_samples_sweep"
  end
end
