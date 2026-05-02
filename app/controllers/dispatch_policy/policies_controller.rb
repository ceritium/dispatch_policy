# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    before_action :find_policy, only: %i[show pause resume drain]

    DRAIN_MAX_PER_REQUEST = 10_000

    def index
      registry_names = DispatchPolicy.registry.names
      db_names       = Partition.distinct.pluck(:policy_name)
      names          = (registry_names + db_names).uniq.sort

      in_flight_by_policy = InflightJob.where(policy_name: names).group(:policy_name).count

      @rows = names.map do |name|
        partitions = Partition.for_policy(name)
        {
          name:           name,
          registered:     registry_names.include?(name),
          pending:        partitions.sum(:pending_count),
          in_flight:      in_flight_by_policy[name] || 0,
          partitions:     partitions.count,
          paused_count:   partitions.paused.count
        }
      end
    end

    def show
      @policy_object = DispatchPolicy.registry.fetch(@policy_name)
      @partitions    = Partition.for_policy(@policy_name)
                                .order(Arel.sql("pending_count DESC, last_admit_at DESC NULLS LAST"))
                                .limit(100)
      @top_admitted  = Partition.for_policy(@policy_name)
                                .order(total_admitted: :desc)
                                .limit(20)

      @totals = {
        pending:    Partition.for_policy(@policy_name).sum(:pending_count),
        in_flight:  InflightJob.where(policy_name: @policy_name).count,
        partitions: Partition.for_policy(@policy_name).count
      }

      now = Time.current
      @windows = {
        "1m"  => Repository.tick_summary(policy_name: @policy_name, since: now - 60),
        "5m"  => Repository.tick_summary(policy_name: @policy_name, since: now - 5 * 60),
        "15m" => Repository.tick_summary(policy_name: @policy_name, since: now - 15 * 60)
      }
      @denied_reasons = Repository.denied_reasons_summary(policy_name: @policy_name, since: now - 15 * 60)
      @round_trip     = Repository.partition_round_trip_stats(policy_name: @policy_name)
      @sparkline      = Repository.tick_samples_buckets(policy_name: @policy_name, since: now - 30 * 60, bucket_seconds: 60)
      @pending_trend  = Repository.trend_direction(@sparkline.map { |b| b[:pending_total] })

      cfg = DispatchPolicy.config
      @capacity = {
        admitted_per_minute:  @windows["1m"][:jobs_admitted],
        adapter_target_jps:   cfg.adapter_throughput_target,
        avg_tick_ms:          @windows["1m"][:avg_duration_ms],
        max_tick_ms:          @windows["1m"][:max_duration_ms],
        tick_max_duration_ms: cfg.tick_max_duration.to_i * 1000
      }

      @hints = OperatorHints.for(
        tick_max_duration_ms: @capacity[:tick_max_duration_ms],
        avg_tick_ms:          @capacity[:avg_tick_ms],
        max_tick_ms:          @capacity[:max_tick_ms],
        pending_total:        @totals[:pending],
        admitted_per_minute:  @capacity[:admitted_per_minute],
        forward_failures:     @windows["1m"][:forward_failures],
        jobs_admitted:        @windows["1m"][:jobs_admitted],
        active_partitions:    @round_trip[:active_partitions],
        never_checked:        @round_trip[:never_checked],
        in_backoff:           @round_trip[:in_backoff],
        total_partitions:     @totals[:partitions],
        adapter_target_jps:   @capacity[:adapter_target_jps],
        pending_trend:        @pending_trend
      )
    end

    def pause
      Partition.for_policy(@policy_name).update_all(status: "paused", updated_at: Time.current)
      redirect_to policy_path(@policy_name), notice: "Policy paused."
    end

    def resume
      Partition.for_policy(@policy_name).update_all(status: "active", updated_at: Time.current)
      redirect_to policy_path(@policy_name), notice: "Policy resumed."
    end

    # Force-admits every staged job across every partition of the policy,
    # bypassing all gates. Walks partitions in pending-DESC order so the
    # busiest ones drain first. Bounded at DRAIN_MAX_PER_REQUEST per click.
    def drain
      drained = 0
      Partition.for_policy(@policy_name)
               .where("pending_count > 0")
               .order(pending_count: :desc, id: :asc)
               .limit(500)
               .each do |partition|
        break if drained >= DRAIN_MAX_PER_REQUEST

        batch, _ = PartitionsController.drain_partition!(partition)
        drained += batch
      end

      remaining = Partition.for_policy(@policy_name).sum(:pending_count)
      notice = if remaining.positive?
        "Drained #{drained} job(s) across this policy; #{remaining} still pending — click drain again to continue."
      else
        "Drained #{drained} job(s); policy fully drained."
      end
      redirect_to policy_path(@policy_name), notice: notice
    end

    private

    def find_policy
      @policy_name = params[:name]
    end
  end
end
