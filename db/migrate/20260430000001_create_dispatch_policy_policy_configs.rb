# frozen_string_literal: true

class CreateDispatchPolicyPolicyConfigs < ActiveRecord::Migration[7.1]
  def change
    # Live, DB-backed per-policy config overrides. Read at TickLoop
    # startup so cluster-wide tunes take effect within
    # tick_max_duration without redeploying. One row per (policy_name,
    # config_key); value is stored as text and decoded by the model
    # (integers and floats only).
    create_table :dispatch_policy_policy_configs do |t|
      t.string   :policy_name, null: false
      t.string   :config_key,  null: false
      t.string   :value,       null: false
      # Source records who wrote this row, useful when debugging
      # an unexpected override:
      #   "code"   — seeded from the Policy DSL on boot (code-wins)
      #   "ui"     — written via console / admin UI
      #   "auto"   — written by the auto_tune loop
      t.string   :source,      null: false, default: "ui"
      t.timestamps
    end

    add_index :dispatch_policy_policy_configs,
      %i[policy_name config_key],
      unique: true,
      name: "idx_dp_policy_configs_unique"
  end
end
