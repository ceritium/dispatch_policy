# frozen_string_literal: true

module DispatchPolicy
  class DashboardController < ApplicationController
    def index
      @totals = {
        staged:        StagedJob.count,
        partitions:    Partition.count,
        active_parts:  Partition.active.count,
        paused_parts:  Partition.paused.count,
        in_flight:     InflightJob.count
      }

      @policies = Partition
        .group(:policy_name)
        .pluck(
          :policy_name,
          Arel.sql("SUM(pending_count)::int"),
          Arel.sql("SUM(in_flight_count)::int"),
          Arel.sql("MAX(last_admit_at)")
        )
        .map { |name, pending, in_flight, last_admit|
          { name: name, pending: pending || 0, in_flight: in_flight || 0, last_admit_at: last_admit }
        }
    end
  end
end
