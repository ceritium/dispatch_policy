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
