# frozen_string_literal: true

module DispatchPolicy
  # One row per Tick.run invocation. Written in batches by
  # TickLoop.run via insert_all so the hot path stays cheap.
  # Read via Stats.tick_runs for p50/p95 dashboards.
  class TickRun < ApplicationRecord
    self.table_name = "dispatch_policy_tick_runs"

    # No updated_at — these rows are immutable samples.
    self.record_timestamps = false

    scope :recent, ->(window = 1.hour) { where("started_at > ?", window.ago) }
  end
end
