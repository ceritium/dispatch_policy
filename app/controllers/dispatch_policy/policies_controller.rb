# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    STALE_PENDING_THRESHOLD = 1.hour

    before_action :load_policy, only: :show

    def index
      @policies = DispatchPolicy.registry.map do |name, job_class|
        scope   = StagedJob.where(policy_name: name)
        pending = scope.pending
        {
          name:            name,
          job_class:       job_class,
          policy:          job_class.resolved_dispatch_policy,
          pending_count:   pending.count,
          admitted_count:  scope.admitted.count,
          completed_24h:   scope.completed.where(completed_at: 24.hours.ago..).count,
          oldest_pending:  pending.minimum(:staged_at),
          stale_threshold: STALE_PENDING_THRESHOLD
        }
      end.sort_by { |p| -p[:pending_count] }

      @active_partitions = PartitionInflightCount.where("in_flight > 0").count
      @expired_leases    = StagedJob.expired_leases.count
    end

    def show
      scope = StagedJob.where(policy_name: @policy_name)
      @pending_count           = scope.pending.count
      @pending_eligible_count  = scope.pending.where("not_before_at IS NULL OR not_before_at <= ?", Time.current).count
      @pending_scheduled_count = @pending_count - @pending_eligible_count
      @admitted_count          = scope.admitted.count
      @completed_24h           = scope.completed.where(completed_at: 24.hours.ago..).count

      @partition_breakdown = partition_breakdown(scope)
      @throttle_buckets = ThrottleBucket
        .where(policy_name: @policy_name).order(:gate_name, :partition_key).limit(50)
      @pending_jobs = scope.pending.order(:priority, :staged_at).limit(50)
    end

    private

    def load_policy
      @policy_name = params[:policy_name]
      @job_class   = DispatchPolicy.registry[@policy_name]
      raise ActiveRecord::RecordNotFound unless @job_class
      @policy = @job_class.resolved_dispatch_policy
    end

    # Per-(gate, partition) breakdown of pending-eligible, pending-scheduled
    # and in-flight counts. Combines rows from PartitionInflightCount
    # (authoritative for admitted in-flight) with partition keys derived
    # from pending rows' context (so partitions that only have pending
    # work are visible too).
    def partition_breakdown(scope)
      gates = (@policy&.gates || []).select(&:partition_by)
      return [] if gates.empty?

      now       = Time.current
      now_iso   = now.iso8601
      since_24h = 24.hours.ago.iso8601

      pending_ctx_counts = scope.pending.group(:context).pluck(
        :context,
        Arel.sql("count(*) filter (where not_before_at is null or not_before_at <= '#{now_iso}')"),
        Arel.sql("count(*) filter (where not_before_at > '#{now_iso}')")
      )

      completed_ctx_counts = scope.completed
        .where("completed_at > ?", since_24h)
        .group(:context).pluck(:context, Arel.sql("count(*)"))

      inflight = PartitionInflightCount.where(policy_name: @policy_name)
        .pluck(:gate_name, :partition_key, :in_flight)
        .each_with_object({}) { |(g, k, n), h| h[[ g, k ]] = n }

      rows = Hash.new { |h, k|
        h[k] = { gate: k[0], partition: k[1], eligible: 0, scheduled: 0, in_flight: 0, completed_24h: 0 }
      }

      gates.each do |gate|
        pending_ctx_counts.each do |ctx, eligible, scheduled|
          partition = gate.partition_key_for((ctx || {}).symbolize_keys)
          row = rows[[ gate.name.to_s, partition ]]
          row[:eligible]  += eligible
          row[:scheduled] += scheduled
        end

        completed_ctx_counts.each do |ctx, completed|
          partition = gate.partition_key_for((ctx || {}).symbolize_keys)
          rows[[ gate.name.to_s, partition ]][:completed_24h] += completed
        end
      end

      inflight.each do |(gate, partition), in_flight|
        rows[[ gate, partition ]][:in_flight] = in_flight
      end

      rows.values.sort_by { |r|
        [ r[:gate], -(r[:eligible] + r[:scheduled] + r[:in_flight]), r[:partition] ]
      }.first(50)
    end
  end
end
