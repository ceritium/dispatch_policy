# frozen_string_literal: true

module DispatchPolicy
  class DashboardController < ApplicationController
    WINDOWS = { "1m" => 60, "5m" => 5 * 60, "15m" => 15 * 60 }.freeze

    def index
      @totals = {
        staged:        StagedJob.count,
        partitions:    Partition.count,
        active_parts:  Partition.active.count,
        paused_parts:  Partition.paused.count,
        in_flight:     InflightJob.count
      }

      now = Time.current
      @windows = WINDOWS.transform_values { |secs| Repository.tick_summary(since: now - secs) }
      @round_trip = Repository.partition_round_trip_stats

      pending_by_policy = Partition
        .group(:policy_name)
        .pluck(:policy_name, Arel.sql("SUM(pending_count)::int"), Arel.sql("MAX(last_admit_at)"))
        .to_h { |name, pending, last_admit| [name, { pending: pending || 0, last_admit_at: last_admit }] }

      in_flight_by_policy = InflightJob.group(:policy_name).count

      one_min_ago = now - 60
      five_min_ago = now - 300

      names = (pending_by_policy.keys + in_flight_by_policy.keys).uniq.sort
      @policies = names.map do |name|
        info  = pending_by_policy[name] || {}
        m1    = Repository.tick_summary(policy_name: name, since: one_min_ago)
        m5    = Repository.tick_summary(policy_name: name, since: five_min_ago)
        rs    = Repository.denied_reasons_summary(policy_name: name, since: one_min_ago)
        rt    = Repository.partition_round_trip_stats(policy_name: name)

        {
          name:           name,
          pending:        info[:pending] || 0,
          in_flight:      in_flight_by_policy[name] || 0,
          last_admit_at:  info[:last_admit_at],
          admitted_1m:    m1[:jobs_admitted],
          admitted_5m:    m5[:jobs_admitted],
          ticks_1m:       m1[:ticks],
          avg_tick_ms_1m: m1[:avg_duration_ms],
          forward_failures_1m: m1[:forward_failures],
          oldest_age_seconds:  rt[:oldest_age_seconds],
          p95_age_seconds:     rt[:p95_age_seconds],
          in_backoff:          rt[:in_backoff],
          top_denial_reason:   rs.first&.first,
          top_denial_count:    rs.first&.last
        }
      end
    end
  end
end
