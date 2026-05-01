# frozen_string_literal: true

module DispatchPolicy
  class TickSample < ApplicationRecord
    self.table_name = "dispatch_policy_tick_samples"

    scope :for_policy, ->(name) { where(policy_name: name) }
    scope :since,      ->(time) { where("sampled_at >= ?", time) }
    scope :recent,     -> { order(sampled_at: :desc) }
  end
end
