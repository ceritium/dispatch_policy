# frozen_string_literal: true

class AddDurationToPartitionObservations < ActiveRecord::Migration[7.1]
  def change
    add_column :dispatch_policy_partition_observations, :total_duration_ms, :bigint,  null: false, default: 0
    add_column :dispatch_policy_partition_observations, :max_duration_ms,   :integer, null: false, default: 0
  end
end
