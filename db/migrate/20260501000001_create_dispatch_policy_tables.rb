# frozen_string_literal: true

class CreateDispatchPolicyTables < ActiveRecord::Migration[7.1]
  def change
    # Payload + ordering store. One row per Job#perform_later until
    # the dispatcher admits it and hands it to the adapter. The
    # partition_key column is the single source of partition truth
    # for both concurrency and throttle gates — they always partition
    # on the same key.
    create_table :dispatch_policy_staged_jobs do |t|
      t.string   :policy_name,      null: false
      t.string   :partition_key,    null: false, default: "default"
      t.string   :job_class,        null: false
      t.jsonb    :arguments,        null: false
      t.jsonb    :context,          null: false, default: {}
      t.integer  :priority,         null: false, default: 100
      t.datetime :not_before_at
      t.datetime :staged_at,        null: false
      t.datetime :admitted_at
      t.datetime :completed_at
      t.datetime :lease_expires_at
      t.string   :active_job_id
      t.string   :dedupe_key

      t.timestamps

      # Per-partition local order — the dispatcher LATERALs into this
      # index, one row per admissible partition.
      t.index %i[policy_name partition_key priority staged_at],
        where: "admitted_at IS NULL",
        name:  "idx_dp_staged_partition_order"

      # Stable dedupe across the lifetime of a "logical" job.
      t.index %i[policy_name dedupe_key],
        unique: true,
        where:  "dedupe_key IS NOT NULL AND completed_at IS NULL",
        name:   "idx_dp_staged_dedupe_active"

      # Lease scan for the reaper.
      t.index :lease_expires_at,
        where: "lease_expires_at IS NOT NULL AND completed_at IS NULL",
        name:  "idx_dp_staged_leases"

      # Adapter <-> staged correlation. ActiveJob's job_id surfaces
      # in the worker's around_perform; we mark_completed_by it.
      t.index :active_job_id,
        where: "active_job_id IS NOT NULL AND completed_at IS NULL",
        name:  "idx_dp_staged_active_job_id"
    end

    # The dispatcher's source of truth. One row per (policy,
    # partition). All gate state — concurrency, throttle bucket,
    # demand, fairness — lives here. The partial idx_partitions_ready
    # IS the ready queue: an admissible partition is one that's in
    # the index. The dispatch query picks from this index in LRU
    # order, LATERALs into staged_jobs for the actual payload, and
    # never even sees a partition that isn't admissible.
    create_table :dispatch_policy_partitions, primary_key: %i[policy_name partition_key] do |t|
      t.string   :policy_name,      null: false
      t.string   :partition_key,    null: false

      # Demand
      t.integer  :pending_count,    null: false, default: 0

      # Concurrency
      t.integer  :in_flight,        null: false, default: 0
      t.integer  :concurrency_max               # NULL = no concurrency gate

      # Throttle (token bucket; refill is computed lazily from refilled_at)
      t.decimal  :tokens,           precision: 14, scale: 6
      t.decimal  :throttle_rate,    precision: 14, scale: 6   # tokens/sec
      t.integer  :throttle_burst
      t.datetime :refilled_at

      # ready = "this partition can admit at least one job right now".
      # Maintained at every write; the partial index below depends on it.
      t.boolean  :ready,            null: false, default: true
      t.datetime :blocked_until

      # Fairness
      t.datetime :last_admitted_at

      t.timestamps
    end

    # The ready queue. Sub-millisecond scan to find the next N
    # admissible partitions in LRU order, regardless of total partition
    # count. NULLS FIRST = "never admitted, give it a turn".
    add_index :dispatch_policy_partitions,
      %i[policy_name last_admitted_at],
      where: "pending_count > 0 AND ready = TRUE",
      name:  "idx_dp_partitions_ready"

    # Cheap unblock sweep — partitions whose blocked_until has passed
    # (typically throttle waiting on a token refill).
    add_index :dispatch_policy_partitions,
      %i[policy_name blocked_until],
      where: "ready = FALSE AND blocked_until IS NOT NULL",
      name:  "idx_dp_partitions_unblock"
  end
end
