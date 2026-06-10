# frozen_string_literal: true

module DispatchPolicy
  # Policy-level settings (currently just the pause flag). One row per
  # policy_name. The tick's claim_partitions consults this so a pause takes
  # effect for partitions created after the pause too — not only the ones
  # that existed when the operator clicked.
  class PolicySetting < ApplicationRecord
    self.table_name = "dispatch_policy_policy_settings"

    scope :for_policy, ->(name) { where(policy_name: name) }
    scope :paused,     -> { where(paused: true) }
  end
end
