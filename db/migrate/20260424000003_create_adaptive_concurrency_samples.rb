# frozen_string_literal: true

class CreateAdaptiveConcurrencySamples < ActiveRecord::Migration[7.1]
  def change
    create_table :dispatch_policy_adaptive_concurrency_samples do |t|
      t.string   :policy_name,     null: false
      t.string   :gate_name,       null: false
      t.string   :partition_key,   null: false
      t.datetime :minute_bucket,   null: false
      t.float    :ewma_latency_ms, null: false, default: 0
      t.integer  :current_max,     null: false

      t.timestamps
    end

    add_index :dispatch_policy_adaptive_concurrency_samples,
      %i[policy_name gate_name partition_key minute_bucket],
      unique: true,
      name: "idx_dp_adaptive_concurrency_samples_unique"

    add_index :dispatch_policy_adaptive_concurrency_samples,
      :minute_bucket,
      name: "idx_dp_adaptive_concurrency_samples_time"
  end
end
