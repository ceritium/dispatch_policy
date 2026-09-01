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
      # One grouped query for pending / partition count / paused count
      # across every policy instead of three per policy.
      counts_by_policy    = Repository.partition_counts_by_policy
      # Policy-level pause flags — the source of truth the tick honors
      # (partitions.status alone misses partitions created after the pause).
      paused_policies     = PolicySetting.paused.pluck(:policy_name).to_set

      @rows = names.map do |name|
        counts = counts_by_policy[name] || {}
        {
          name:           name,
          registered:     registry_names.include?(name),
          paused:         paused_policies.include?(name),
          pending:        counts[:pending] || 0,
          in_flight:      in_flight_by_policy[name] || 0,
          partitions:     counts[:partitions] || 0,
          paused_count:   counts[:paused] || 0
        }
      end
    end

    def show
      @policy_object = DispatchPolicy.registry.fetch(@policy_name)
      @paused        = PolicySetting.for_policy(@policy_name).pick(:paused) || false
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
        partitions_seen:      @windows["1m"][:partitions_seen],
        active_partitions:    @round_trip[:active_partitions],
        never_checked:        @round_trip[:never_checked],
        in_backoff:           @round_trip[:in_backoff],
        total_partitions:     @totals[:partitions],
        adapter_target_jps:   @capacity[:adapter_target_jps],
        pending_trend:        @pending_trend,
        paused:               @paused
      )
    end

    def pause
      # Two writes, deliberately NOT in one transaction, and deliberately
      # in this order.
      #
      # The policy-level flag is the source of truth the tick honors — its
      # claim skips a paused policy outright, including partitions that
      # first appear AFTER the pause (audit M6). The per-partition status
      # is the second, redundant mechanism the partitions list renders.
      #
      # They used to share a transaction so neither could land alone. What
      # that actually bought was a single failure that loses BOTH: the
      # status flip writes every partition row of the policy with no lock
      # order of its own, so it deadlocks against an ordinary bulk enqueue
      # (5 in 12 clicks, measured), Postgres kills this transaction, and
      # the pause silently does not happen while the request 500s. During
      # the load that made the operator click pause, which is the only time
      # anyone clicks it.
      #
      # `Repository.set_partitions_status!` now takes its row locks in the
      # canonical byte order, which is what removes the deadlock; it slices
      # so a large policy does not hold every lock at once, and that gives
      # up all-or-nothing. Writing the flag FIRST is what makes the give-up
      # safe: every partial state is "paused, and some rows not yet marked"
      # — admission is already stopped. The reverse order could leave
      # admission running with a UI that says paused.
      Repository.set_policy_paused!(policy_name: @policy_name, paused: true)
      Repository.set_partitions_status!(policy_name: @policy_name, status: "paused")
      redirect_to policy_path(@policy_name), notice: "Policy paused."
    end

    def resume
      # Mirror image, for the same reason: statuses first, flag last. A
      # partial resume then leaves the policy paused (the flag still holds
      # admission), never partitions marked active under a flag nobody
      # cleared. Both directions fail closed.
      Repository.set_partitions_status!(policy_name: @policy_name, status: "active")
      Repository.set_policy_paused!(policy_name: @policy_name, paused: false)
      redirect_to policy_path(@policy_name), notice: "Policy resumed."
    end

    # Force-admits every staged job across every partition of the policy,
    # bypassing all gates. Walks partitions in pending-DESC order so the
    # busiest ones drain first. Bounded at DRAIN_MAX_PER_REQUEST per click.
    def drain
      drained  = 0
      failures = 0
      Partition.for_policy(@policy_name)
               .where("pending_count > 0")
               .order(pending_count: :desc, id: :asc)
               .limit(500)
               .each do |partition|
        break if drained >= DRAIN_MAX_PER_REQUEST

        # Pass the REMAINING budget so a single partition can't push the
        # total past the cap (a fixed per-partition cap could overshoot by
        # nearly 2× when the first partition nearly fills it).
        batch, _due, _scheduled, failed =
          PartitionsController.drain_partition!(partition, cap: DRAIN_MAX_PER_REQUEST - drained)
        drained  += batch
        failures += 1 if failed
      end

      remaining = Partition.for_policy(@policy_name).sum(:pending_count)
      notice = if failures.positive?
        "Drained #{drained} job(s); #{failures} partition(s) could not be forwarded — see logs."
      elsif remaining.positive?
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
