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
      @job_class   = DispatchPolicy.registry[@policy_name] ||
                     Tick.autoload_job_for(@policy_name)
      raise ActiveRecord::RecordNotFound unless @job_class
      @policy = @job_class.resolved_dispatch_policy
    end

    # Per-(source, partition) breakdown of pending-eligible / pending-scheduled
    # / in-flight / completed-24h. A "source" is either a gate with a
    # partition_by (uses gate.partition_key_for(context)) or the policy's
    # round_robin_by declaration (uses the round_robin_key column directly).
    # All four counts come from StagedJob groupings; PartitionInflightCount
    # is an admission-time optimization, not the user-facing truth.
    def partition_breakdown(scope)
      sources = partition_sources
      return [] if sources.empty?

      now       = Time.current
      now_iso   = now.iso8601
      since_24h = 24.hours.ago.iso8601

      rows = Hash.new { |h, k|
        h[k] = { source: k[0], partition: k[1], eligible: 0, scheduled: 0, in_flight: 0, completed_24h: 0 }
      }

      sources.each do |name, extract|
        pending_counts = scope.pending.group(:context, :round_robin_key).pluck(
          :context,
          :round_robin_key,
          Arel.sql("count(*) filter (where not_before_at is null or not_before_at <= '#{now_iso}')"),
          Arel.sql("count(*) filter (where not_before_at > '#{now_iso}')")
        )
        pending_counts.each do |ctx, rr_key, eligible, scheduled|
          partition = extract.call(ctx, rr_key)
          row = rows[[ name, partition ]]
          row[:eligible]  += eligible
          row[:scheduled] += scheduled
        end

        admitted_counts = scope.admitted.group(:context, :round_robin_key).pluck(
          :context, :round_robin_key, Arel.sql("count(*)")
        )
        admitted_counts.each do |ctx, rr_key, in_flight|
          partition = extract.call(ctx, rr_key)
          rows[[ name, partition ]][:in_flight] += in_flight
        end

        completed_counts = scope.completed.where("completed_at > ?", since_24h)
          .group(:context, :round_robin_key).pluck(
            :context, :round_robin_key, Arel.sql("count(*)")
          )
        completed_counts.each do |ctx, rr_key, completed|
          partition = extract.call(ctx, rr_key)
          rows[[ name, partition ]][:completed_24h] += completed
        end
      end

      rows.values
        .reject { |r| r[:partition].nil? || r[:partition].empty? }
        .sort_by { |r| [ r[:source], -(r[:eligible] + r[:scheduled] + r[:in_flight]), r[:partition] ] }
        .first(50)
    end

    # Returns [[source_name, ->(ctx, rr_key) { partition_key }], ...]
    # covering every partition-producing declaration on the policy: every
    # gate with a partition_by, plus round_robin_by if declared.
    def partition_sources
      return [] unless @policy

      sources = @policy.gates.select(&:partition_by).map do |gate|
        [ gate.name.to_s, ->(ctx, _rr) { gate.partition_key_for((ctx || {}).symbolize_keys) } ]
      end
      sources << [ "round_robin_by", ->(_ctx, rr) { rr } ] if @policy.round_robin?
      sources
    end
  end
end
