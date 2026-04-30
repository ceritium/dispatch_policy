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
    # Fairness specifically counts ONLY LRU-lagged partitions — those
    # waiting their LRU turn in spite of all gates having headroom.
    # Partitions blocked by an at-cap concurrency / adaptive /
    # throttle / global_cap gate are reported under
    # `fairness[:gate_blocked]` (informational, not a verdict trigger)
    # because that backlog is by design — the operator chose those
    # caps.
    def slo(policy_name,
            latency_budget_seconds: 60,
            fairness_threshold_seconds: 60,
            throughput_window_seconds: 60)
      now        = Time.current
      batch_size = DispatchPolicy.config.batch_size

      oldest_staged_at = StagedJob.pending.where(policy_name: policy_name)
                                  .minimum(:staged_at)
      oldest_age = oldest_staged_at ? (now - oldest_staged_at).round(2) : nil

      fairness = fairness_breakdown(policy_name, fairness_threshold_seconds)

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
        fairness: fairness.merge(threshold: fairness_threshold_seconds),
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

    # Per-policy bottleneck diagnosis. Splits stale partitions into
    # "gate_blocked" (intentional — operator's choice) and "lru_lagged"
    # (dispatch_policy can't keep up — tunable). Returns the dominant
    # diagnosis kind plus the config knobs that address it.
    #
    #   DispatchPolicy::Stats.bottleneck("send_webhook_job")
    #   # => {
    #   #   policy_name: "send_webhook_job",
    #   #   applicable: true,
    #   #   stale_partitions: { gate_blocked: 12, lru_lagged: 3 },
    #   #   diagnosis: :rotation_lag,
    #   #   suggested_knobs: [:round_robin_quantum,
    #   #                     :round_robin_max_partitions_per_tick]
    #   # }
    #
    # Returns `applicable: false` for policies without round_robin_by
    # (no partition_states rows, no rotation to lag).
    def bottleneck(policy_name, fairness_threshold_seconds: 60)
      policy = registry_lookup(policy_name)
      return { policy_name: policy_name, applicable: false, diagnosis: :ok } unless policy && policy.round_robin?

      breakdown = fairness_breakdown(policy_name, fairness_threshold_seconds)
      gate_blocked = breakdown[:gate_blocked]
      lru_lagged   = breakdown[:stale_partitions]
      active       = PartitionState.where(policy_name: policy_name)
                                   .where("pending_count > 0").count

      current_batch_size = policy.effective_batch_size
      current_quantum    = policy.effective_round_robin_quantum
      current_cap        = policy.effective_round_robin_max_partitions_per_tick

      diagnosis, knobs, recommended =
        if lru_lagged.zero?
          [ :ok, [], {} ]
        elsif active > current_batch_size
          # More active partitions than fit in one tick — widen the
          # batch first, then drop quantum to make every plucked
          # partition contribute.
          recommended_batch = [ next_size_step(active), 5_000 ].min
          [
            :capacity_strain,
            %i[batch_size round_robin_quantum round_robin_max_partitions_per_tick],
            {
              batch_size:          { current: current_batch_size, suggested: recommended_batch },
              round_robin_quantum: { current: current_quantum,    suggested: 1 }
            }
          ]
        else
          # Active partitions fit but rotation isn't reaching them all.
          # Lower quantum is the cleanest knob.
          [
            :rotation_lag,
            %i[round_robin_quantum round_robin_max_partitions_per_tick],
            {
              round_robin_quantum: { current: current_quantum, suggested: 1 }
            }
          ]
        end

      {
        policy_name:        policy_name,
        applicable:         true,
        stale_partitions:   { gate_blocked: gate_blocked, lru_lagged: lru_lagged },
        diagnosis:          diagnosis,
        suggested_knobs:    knobs,
        recommended_config: recommended,
        current_config:     {
          batch_size:                          current_batch_size,
          round_robin_quantum:                 current_quantum,
          round_robin_max_partitions_per_tick: current_cap,
          lease_duration:                      policy.effective_lease_duration
        }
      }
    end

    # Round `n` up to the next sensible "operator value": 100, 200, 500,
    # 1000, 2000, 5000. Avoids suggestions like batch_size=237.
    def next_size_step(n)
      [ 100, 200, 500, 1000, 2000, 5000 ].find { |v| v >= n } || 5_000
    end
    private_class_method :next_size_step

    # Coarse health signal: returns :ok or a Symbol describing the
    # first dispatch_policy-tunable concern. Intentional gate caps
    # (concurrency at max, throttle exhausted, global_cap saturated)
    # do NOT trigger this — those are operator-chosen limits, not
    # dispatch_policy bottlenecks.
    #
    #   :ok             — no LRU rotation lag and no expired leases
    #   :rotation_lag   — at least one partition with pending rows is
    #                     waiting its LRU turn while all gates have
    #                     headroom for it. Tune quantum / cap / batch_size.
    #   :inflight_drift — admitted rows past their lease (worker
    #                     crashes outpacing the reaper). Tune
    #                     lease_duration or investigate workers.
    def health(stale_threshold_seconds: nil)
      stale_threshold = stale_threshold_seconds || (DispatchPolicy.config.lease_duration * 3)

      DispatchPolicy.registry.keys.each do |policy_name|
        breakdown = fairness_breakdown(policy_name, stale_threshold)
        return :rotation_lag if breakdown[:stale_partitions].positive?
      end

      return :inflight_drift if StagedJob.expired_leases.exists?

      :ok
    end

    # ─── Internal helpers ─────────────────────────────────────────

    # Returns { stale_partitions: int, gate_blocked: int, ok: bool }.
    # `stale_partitions` is the LRU-lagged count (operator-tunable).
    # `gate_blocked` is informational — partitions waiting because a
    # gate cap is reached.
    def fairness_breakdown(policy_name, threshold_seconds)
      stale_keys = PartitionState
        .where(policy_name: policy_name)
        .where("pending_count > 0")
        .where("COALESCE(last_admitted_at, created_at) < ?", threshold_seconds.seconds.ago)
        .pluck(:partition_key)

      if stale_keys.empty?
        return { stale_partitions: 0, gate_blocked: 0, ok: true }
      end

      policy = registry_lookup(policy_name)
      blocked_keys = stale_keys.empty? || policy.nil? ? [] : gate_blocked_keys(policy, stale_keys)

      {
        stale_partitions: stale_keys.size - blocked_keys.size,
        gate_blocked:     blocked_keys.size,
        ok:               (stale_keys.size - blocked_keys.size).zero?
      }
    end
    private_class_method :fairness_breakdown

    # Among `stale_keys` for `policy`, return the subset that has at
    # least one tracks_inflight gate at its cap (or throttle out of
    # tokens, or global_cap saturated). Those are intentional waits,
    # not LRU lag.
    def gate_blocked_keys(policy, stale_keys)
      policy_name = policy.name
      blocked = Set.new

      # Throttle: tokens < 1 means rejected at admission.
      ThrottleBucket
        .where(policy_name: policy_name, partition_key: stale_keys)
        .where("tokens < 1")
        .pluck(:partition_key)
        .each { |pk| blocked << pk }

      # Concurrency / Global / Adaptive: compare in_flight against the
      # gate's effective max. Pull all in one query, group by partition.
      in_flight_rows = PartitionInflightCount
        .where(policy_name: policy_name, partition_key: stale_keys)
        .pluck(:gate_name, :partition_key, :in_flight)
      in_flight_by_pk = in_flight_rows.group_by { |_, pk, _| pk }
        .transform_values { |rows| rows.to_h { |gn, _, inf| [ gn, inf ] } }

      adaptive_max = AdaptiveConcurrencyStats
        .where(policy_name: policy_name, partition_key: stale_keys)
        .pluck(:gate_name, :partition_key, :current_max)
        .each_with_object({}) { |(gn, pk, cm), h| h[[ pk, gn ]] = cm }

      # global_cap is a single number across all partitions for the
      # policy. If the policy's total in_flight ≥ gate.max, every
      # partition is blocked by it.
      global_cap_blocked_all = false
      policy.gates.each do |gate|
        next unless gate.is_a?(Gates::GlobalCap)
        cap = gate_max_value(gate.max)
        next unless cap
        total = PartitionInflightCount
          .where(policy_name: policy_name, gate_name: gate.name.to_s)
          .sum(:in_flight)
        global_cap_blocked_all = true if total >= cap
      end

      stale_keys.each do |pk|
        next if blocked.include?(pk)
        if global_cap_blocked_all
          blocked << pk
          next
        end

        gate_inflight = in_flight_by_pk[pk] || {}
        hit_cap = policy.gates.any? do |gate|
          next false unless gate.tracks_inflight?
          gn = gate.name.to_s
          cur = gate_inflight[gn].to_i
          cap = case gate
          when Gates::Concurrency
            gate_max_value(gate.max)
          when Gates::AdaptiveConcurrency
            adaptive_max[[ pk, gn ]] || gate_max_value(gate.initial_max)
          else
            nil
          end
          cap && cur >= cap
        end
        blocked << pk if hit_cap
      end

      blocked.to_a
    end
    private_class_method :gate_blocked_keys

    # The cap can be a literal Integer or a Proc/lambda (resolved at
    # filter time with a context). For diagnostic purposes we only
    # interpret literals — Proc-bound caps fall through to "lru_lagged"
    # rather than risk evaluating without context.
    def gate_max_value(max)
      return nil if max.nil?
      return max if max.is_a?(Integer)
      nil
    end
    private_class_method :gate_max_value

    # Lookup a policy by name via the dispatchable job registry.
    # Returns nil if the job class hasn't been autoloaded yet.
    def registry_lookup(policy_name)
      job_class = DispatchPolicy.registry[policy_name]
      job_class&.resolved_dispatch_policy
    end
    private_class_method :registry_lookup
  end
end
