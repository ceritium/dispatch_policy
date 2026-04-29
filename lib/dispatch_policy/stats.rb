# frozen_string_literal: true

module DispatchPolicy
  # Operator-facing snapshot of the gem's runtime state. Cheap enough
  # to call from health endpoints, console, or external scrapers.
  # Pair with the AS::Notifications events
  # ("tick.dispatch_policy" / "reap.dispatch_policy") for hot-path
  # metrics that should be exported to Prometheus / StatsD / etc.
  module Stats
    module_function

    # Per-policy state snapshot. Returns a Hash with backlog, fairness,
    # and capacity counters that are useful for dashboards and alerts.
    #
    #   DispatchPolicy::Stats.policy_summary("send_webhook_job")
    #   # => {
    #   #   policy_name: "send_webhook_job",
    #   #   pending: 1234,
    #   #   admitted: 12,
    #   #   completed_24h: 88_213,
    #   #   active_partitions: 540,
    #   #   drained_partitions: 19_460,
    #   #   oldest_pending_age_seconds: 0.83,
    #   #   stale_partitions_60s: 3,
    #   #   stale_partitions_300s: 0
    #   # }
    def policy_summary(policy_name, stale_thresholds: [ 60, 300 ])
      now = Time.current

      pending  = StagedJob.pending.where(policy_name: policy_name).count
      admitted = StagedJob.admitted.where(policy_name: policy_name).count
      completed_24h = StagedJob.where(policy_name: policy_name)
                              .where("completed_at > ?", 24.hours.ago).count

      active_partitions  = PartitionState.where(policy_name: policy_name)
                                         .where("pending_count > 0").count
      drained_partitions = PartitionState.where(policy_name: policy_name)
                                         .where(pending_count: 0).count

      oldest_staged_at = StagedJob.pending.where(policy_name: policy_name)
                                  .minimum(:staged_at)
      oldest_age = oldest_staged_at ? (now - oldest_staged_at).round(2) : nil

      stale_counts = stale_thresholds.each_with_object({}) do |seconds, acc|
        acc[:"stale_partitions_#{seconds}s"] = PartitionState
          .where(policy_name: policy_name)
          .where("pending_count > 0")
          # COALESCE: never-admitted partitions count as stale only if
          # they've been visible (created_at) longer than the threshold.
          # A freshly staged partition isn't immediately "stale" — it
          # just hasn't had its turn yet.
          .where("COALESCE(last_admitted_at, created_at) < ?", seconds.seconds.ago)
          .count
      end

      {
        policy_name:                policy_name,
        pending:                    pending,
        admitted:                   admitted,
        completed_24h:              completed_24h,
        active_partitions:          active_partitions,
        drained_partitions:         drained_partitions,
        oldest_pending_age_seconds: oldest_age
      }.merge(stale_counts)
    end

    # Snapshot for every policy known to the registry. One row per
    # policy. Use this to drive a JSON endpoint or render the admin
    # UI's overview page.
    def summary(stale_thresholds: [ 60, 300 ])
      DispatchPolicy.registry.keys.map { |name| policy_summary(name, stale_thresholds: stale_thresholds) }
    end

    # Per-policy SLO snapshot with operator-supplied budgets. Returns
    # a Hash with the four canonical signals dispatch_policy recommends
    # tracking: latency, fairness, throughput, capacity. Each (where
    # applicable) carries an `ok:` flag the operator can OR-reduce
    # for an alert. Throughput has no `ok:` because only the operator
    # knows their ingress rate; we expose the measured value and the
    # window so they can compute the comparison themselves.
    #
    #   DispatchPolicy::Stats.slo("send_webhook_job",
    #     latency_budget_seconds: 30,
    #     fairness_threshold_seconds: 60,
    #     throughput_window_seconds: 60)
    def slo(policy_name,
            latency_budget_seconds: 60,
            fairness_threshold_seconds: 60,
            throughput_window_seconds: 60)
      now        = Time.current
      batch_size = DispatchPolicy.config.batch_size

      oldest_staged_at = StagedJob.pending.where(policy_name: policy_name)
                                  .minimum(:staged_at)
      oldest_age = oldest_staged_at ? (now - oldest_staged_at).round(2) : nil

      stale = PartitionState
        .where(policy_name: policy_name)
        .where("pending_count > 0")
        .where("COALESCE(last_admitted_at, created_at) < ?", fairness_threshold_seconds.seconds.ago)
        .count

      admissions_in_window = StagedJob.where(policy_name: policy_name)
        .where("admitted_at > ?", throughput_window_seconds.seconds.ago)
        .count
      throughput_per_sec = (admissions_in_window.to_f / throughput_window_seconds).round(2)

      active = PartitionState.where(policy_name: policy_name)
                             .where("pending_count > 0").count

      {
        policy_name: policy_name,
        latency: {
          seconds: oldest_age,
          budget:  latency_budget_seconds,
          ok:      oldest_age.nil? || oldest_age <= latency_budget_seconds
        },
        fairness: {
          stale_partitions: stale,
          threshold:        fairness_threshold_seconds,
          ok:               stale.zero?
        },
        throughput: {
          admissions_per_sec: throughput_per_sec,
          window_seconds:     throughput_window_seconds
        },
        capacity: {
          active_partitions: active,
          batch_size:        batch_size,
          utilization:       active.zero? ? 0.0 : (active.to_f / batch_size).round(2),
          headroom:          active < batch_size
        }
      }
    end

    # SLO snapshot for every registered policy. Drop into a
    # `/dispatch_policy/slos.json` endpoint or scrape periodically.
    def slos(**budgets)
      DispatchPolicy.registry.keys.map { |name| slo(name, **budgets) }
    end

    # Coarse health signal: returns :ok or a Symbol describing the
    # first concern. Useful as a single value for alerting.
    #
    #   :ok                           — nothing notable
    #   :starvation                   — at least one active partition
    #                                   has no admission within
    #                                   stale_threshold_seconds (default
    #                                   3 × lease_duration)
    #   :backlog_aging                — oldest pending row > backlog_age_seconds
    #   :inflight_drift               — admitted rows past their lease
    def health(stale_threshold_seconds: nil, backlog_age_seconds: 5 * 60)
      stale_threshold = stale_threshold_seconds || (DispatchPolicy.config.lease_duration * 3)

      now = Time.current
      starving = PartitionState
        .where("pending_count > 0")
        .where("COALESCE(last_admitted_at, created_at) < ?", stale_threshold.seconds.ago)
        .exists?
      return :starvation if starving

      old_pending = StagedJob.pending.where("staged_at < ?", backlog_age_seconds.seconds.ago).exists?
      return :backlog_aging if old_pending

      stuck_inflight = StagedJob.expired_leases.exists?
      return :inflight_drift if stuck_inflight

      :ok
    end
  end
end
