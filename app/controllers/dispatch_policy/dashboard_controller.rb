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

      pending_by_policy = Partition
        .group(:policy_name)
        .pluck(:policy_name, Arel.sql("SUM(pending_count)::int"), Arel.sql("MAX(last_admit_at)"))
        .to_h { |name, pending, last_admit| [name, { pending: pending || 0, last_admit_at: last_admit }] }

      in_flight_by_policy = InflightJob.group(:policy_name).count

      names = (pending_by_policy.keys + in_flight_by_policy.keys).uniq.sort
      @policies = names.map do |name|
        info = pending_by_policy[name] || {}
        {
          name:           name,
          pending:        info[:pending] || 0,
          in_flight:      in_flight_by_policy[name] || 0,
          last_admit_at:  info[:last_admit_at]
        }
      end
    end
  end
end
