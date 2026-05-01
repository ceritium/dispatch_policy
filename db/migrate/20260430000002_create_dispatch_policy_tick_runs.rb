# frozen_string_literal: true

class CreateDispatchPolicyTickRuns < ActiveRecord::Migration[7.1]
  def change
    # One row per Tick.run invocation. Buffered in memory by
    # TickLoop.run and flushed in batches via insert_all so the
    # hot path doesn't pay an extra DB round-trip per tick. Use
    # for performance dashboards (p50/p95 duration, admitted
    # rate, error rate) and for retroactive debugging when the
    # TickLoop misbehaves on a specific deploy.
    create_table :dispatch_policy_tick_runs do |t|
      t.string   :policy_name                               # nil = all policies
      t.datetime :started_at,   null: false
      t.float    :duration_ms,  null: false
      t.integer  :admitted,     null: false, default: 0
      t.integer  :partitions,   null: false, default: 0
      t.boolean  :declined,     null: false, default: false
      t.string   :error_class                               # set when Tick.run raised
      t.string   :error_message
      t.datetime :created_at,   null: false

      # Cursor health. cursor_lag_ms = NOW() - oldest last_checked_at
      # among visited partitions, i.e. how long the most-stale
      # partition had been waiting since its last turn. The avg /
      # p95 over time is the dispatcher cycle time.
      t.float    :cursor_lag_ms
      t.integer  :active_partitions, null: false, default: 0   # active = pending_count>0 at the start of the tick

      # Inline so the indexes are part of the empty CREATE TABLE —
      # safe without concurrent ADD INDEX (the table has no rows).
      t.index :started_at,
        name: "idx_dp_tick_runs_started_at"
      t.index %i[policy_name started_at],
        name: "idx_dp_tick_runs_policy_started"
    end
  end
end
