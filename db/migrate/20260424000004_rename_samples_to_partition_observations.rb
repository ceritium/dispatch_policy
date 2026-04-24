# frozen_string_literal: true

class RenameSamplesToPartitionObservations < ActiveRecord::Migration[7.1]
  def up
    drop_table :dispatch_policy_adaptive_concurrency_samples, if_exists: true

    create_table :dispatch_policy_partition_observations do |t|
      t.string   :policy_name,       null: false
      t.string   :partition_key,     null: false
      t.datetime :minute_bucket,     null: false
      t.bigint   :total_lag_ms,      null: false, default: 0
      t.integer  :observation_count, null: false, default: 0
      t.integer  :max_lag_ms,        null: false, default: 0
      t.integer  :current_max

      t.timestamps
    end

    add_index :dispatch_policy_partition_observations,
      %i[policy_name partition_key minute_bucket],
      unique: true,
      name: "idx_dp_partition_observations_unique"

    add_index :dispatch_policy_partition_observations,
      :minute_bucket,
      name: "idx_dp_partition_observations_time"
  end

  def down
    drop_table :dispatch_policy_partition_observations
  end
end
