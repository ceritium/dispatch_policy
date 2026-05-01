# frozen_string_literal: true

module DispatchPolicy
  # Operator-facing snapshot of dispatcher state. All numbers come
  # from `dispatch_policy_partitions` and `dispatch_policy_tick_runs`
  # — no fan-out across multiple tables — so each call is a few
  # straight queries.
  module Stats
    module_function

    # Per-policy summary: pending demand, admissions in flight, and
    # how partitions break down by why they're not currently ready.
    def policy_summary(policy_name)
      now = Time.current

      rows = PolicyPartition.where(policy_name: policy_name).pluck(
        :pending_count, :in_flight, :concurrency_max,
        :tokens, :throttle_rate, :throttle_burst, :refilled_at,
        :ready, :blocked_until, :last_admitted_at
      )

      pending  = rows.sum { |r| r[0].to_i }
      inflight = rows.sum { |r| r[1].to_i }
      partitions = rows.size

      buckets = Hash.new(0)
      rows.each do |pc, inf, cmax, tok, rate, _burst, refilled, ready, blocked_until, _last|
        if pc.zero? && inf.zero?
          buckets[:idle] += 1
          next
        end
        if ready
          buckets[:ready] += 1
        elsif cmax && inf >= cmax
          buckets[:concurrency_blocked] += 1
        elsif rate
          buckets[:throttle_blocked] += 1
        else
          buckets[:other] += 1
        end
      end

      {
        policy_name:           policy_name,
        pending:               pending,
        in_flight:             inflight,
        partition_count:       partitions,
        ready_partitions:      buckets[:ready],
        concurrency_blocked:   buckets[:concurrency_blocked],
        throttle_blocked:      buckets[:throttle_blocked],
        idle_partitions:       buckets[:idle]
      }
    end

    def summary
      DispatchPolicy.registry.keys.map { |name| policy_summary(name) }
    end

    # Per-policy SLO snapshot. Latency = oldest pending staged_at age.
    # Fairness = how many ready partitions we have vs blocked.
    def slo(policy_name, latency_budget_seconds: 60, fairness_threshold_seconds: 60)
      summary = policy_summary(policy_name)

      oldest_staged_at = StagedJob.pending.where(policy_name: policy_name).minimum(:staged_at)
      oldest_age = oldest_staged_at ? (Time.current - oldest_staged_at).round(2) : nil

      pending_with_lag = StagedJob.pending
        .where(policy_name: policy_name)
        .where("staged_at < ?", fairness_threshold_seconds.seconds.ago)
        .count

      {
        policy_name:  policy_name,
        latency: {
          seconds: oldest_age,
          budget:  latency_budget_seconds,
          ok:      oldest_age.nil? || oldest_age <= latency_budget_seconds
        },
        fairness: {
          ready_partitions:    summary[:ready_partitions],
          concurrency_blocked: summary[:concurrency_blocked],
          throttle_blocked:    summary[:throttle_blocked],
          stale_pending:       pending_with_lag,
          threshold:           fairness_threshold_seconds,
          # ok = there's no demand stuck behind a non-gate reason. With
          # the new dispatcher, "lru lag" is impossible — if a partition
          # is ready, the next tick admits it. Therefore stale_pending
          # is fully explained by gate-blocked partitions.
          ok:                  pending_with_lag.zero? || summary[:ready_partitions].zero?
        },
        capacity: {
          ready_partitions: summary[:ready_partitions],
          batch_size:       DispatchPolicy.registry[policy_name]&.resolved_dispatch_policy&.effective_batch_size
        }
      }
    end

    def slos(**budgets)
      DispatchPolicy.registry.keys.map { |name| slo(name, **budgets) }
    end

    # Bottleneck diagnosis. With concurrency + throttle, the only
    # operator-tunable bottleneck dispatch_policy can introduce is a
    # too-small batch_size relative to demand. Concurrency / throttle
    # being at-cap is by design.
    def bottleneck(policy_name)
      policy  = DispatchPolicy.registry[policy_name]&.resolved_dispatch_policy
      summary = policy_summary(policy_name)

      ready  = summary[:ready_partitions]
      pending = summary[:pending]
      batch_size = policy&.effective_batch_size || DispatchPolicy.config.batch_size

      diagnosis, recommended =
        if pending.zero? || ready.zero?
          [ :ok, {} ]
        elsif ready > batch_size
          # Demand fits in dispatcher, but a single tick can't drain
          # all ready partitions. Suggest a larger batch.
          suggested = next_size_step(ready)
          [ :capacity_strain, { batch_size: { current: batch_size, suggested: suggested } } ]
        else
          [ :ok, {} ]
        end

      {
        policy_name:        policy_name,
        applicable:         true,
        diagnosis:          diagnosis,
        partition_state:    {
          ready:               summary[:ready_partitions],
          concurrency_blocked: summary[:concurrency_blocked],
          throttle_blocked:    summary[:throttle_blocked]
        },
        recommended_config: recommended,
        current_config: {
          batch_size:     batch_size,
          lease_duration: policy&.effective_lease_duration
        }
      }
    end

    def next_size_step(n)
      [ 100, 200, 500, 1000, 2000, 5000 ].find { |v| v >= n } || 5_000
    end
    private_class_method :next_size_step

    # Coarse health: :ok or :inflight_drift (lease-expired admissions
    # waiting for the reaper). Pure dispatcher concerns; gate caps
    # never trip this signal.
    def health
      return :inflight_drift if StagedJob.expired_leases.exists?
      :ok
    end

    # ─── TickLoop perf samples ───────────────────────────────────

    def tick_runs(window: 300, policy_name: nil)
      scope = TickRun.where("started_at > ?", window.seconds.ago)
      scope = scope.where(policy_name: policy_name) if policy_name

      rows = scope.pluck(:policy_name, :duration_ms, :admitted, :partitions, :declined, :error_class)
      overall = aggregate_tick_rows(rows)
      per_policy = rows.group_by { |r| r[0] }.transform_values { |g| aggregate_tick_rows(g) }

      overall.merge(window_seconds: window, per_policy: per_policy)
    end

    def aggregate_tick_rows(rows)
      ticks = rows.size
      return blank_tick_summary(ticks) if ticks.zero?

      durations = rows.map { |r| r[1].to_f }.sort
      {
        ticks:      ticks,
        admitted:   rows.sum { |r| r[2].to_i },
        partitions: rows.sum { |r| r[3].to_i },
        p50_ms:     percentile(durations, 0.5),
        p95_ms:     percentile(durations, 0.95),
        p99_ms:     percentile(durations, 0.99),
        max_ms:     durations.last.round(2),
        errored:    rows.count { |r| r[5].present? },
        declined:   rows.count { |r| r[4] }
      }
    end
    private_class_method :aggregate_tick_rows

    def blank_tick_summary(ticks)
      {
        ticks: ticks, admitted: 0, partitions: 0,
        p50_ms: nil, p95_ms: nil, p99_ms: nil, max_ms: nil,
        errored: 0, declined: 0
      }
    end
    private_class_method :blank_tick_summary

    def percentile(sorted, q)
      return sorted.first.round(2) if sorted.size == 1
      idx = (sorted.size - 1) * q
      lo  = sorted[idx.floor]
      hi  = sorted[idx.ceil]
      (lo + (hi - lo) * (idx - idx.floor)).round(2)
    end
    private_class_method :percentile
  end
end
