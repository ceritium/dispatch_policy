# frozen_string_literal: true

class CreateAdaptiveConcurrencyStats < ActiveRecord::Migration[7.1]
  def change
    create_table :dispatch_policy_adaptive_concurrency_stats do |t|
      t.string   :policy_name,     null: false
      t.string   :gate_name,       null: false
      t.string   :partition_key,   null: false, default: "default"
      t.integer  :current_max,     null: false
      t.float    :ewma_latency_ms, null: false, default: 0
      t.integer  :sample_count,    null: false, default: 0
      t.datetime :last_observed_at

      t.timestamps
    end

    add_index :dispatch_policy_adaptive_concurrency_stats,
      %i[policy_name gate_name partition_key],
      unique: true,
      name: "idx_dp_adaptive_concurrency_stats_unique"
  end
end
