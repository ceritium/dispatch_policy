# frozen_string_literal: true

module DispatchPolicy
  # Drives admission until `stop_when` fires (deadline, shutdown signal, etc).
  # Runs one Tick per policy per loop iteration; sleeps `idle_pause` when no
  # jobs were admitted across all policies. Periodically (every
  # `sweep_every_ticks` iterations) sweeps stale inflight rows and inactive
  # partitions.
  module TickLoop
    module_function

    # @param policy_name [String, nil] limit to one policy. nil = all registered.
    # @param shard [String, nil] limit to one shard. nil = all shards.
    def run(policy_name: nil, shard: nil, stop_when: -> { false })
      config       = DispatchPolicy.config
      logger       = config.logger
      iteration    = 0

      loop do
        break if stop_when.call

        # NOTE: `config.enabled` is deliberately not consulted here. It
        # governs the ENQUEUE side — whether new perform_later calls are
        # intercepted — and turning it off is how you stop taking new
        # traffic into staging during a cutover. Work already staged still
        # has to come out: the tick is the only thing that admits it, and
        # with staging disabled nothing else will ever put those rows back
        # into the adapter. Breaking out of this loop stranded the backlog
        # where only the dashboard's drain button could reach it, which is
        # the opposite of the "drain the staging table without taking
        # traffic offline" the option exists for. To actually stop
        # admitting, stop the tick job or pause the policy from the UI
        # (which claim_partitions honors).
        names = policy_names(policy_name)
        if names.empty?
          pause(config.idle_pause)
          next
        end

        admitted = 0
        names.each do |name|
          break if stop_when.call

          begin
            result = Tick.run(policy_name: name, shard: shard)
            admitted += result.jobs_admitted
          rescue StandardError => e
            logger&.error("[dispatch_policy] tick error policy=#{name} shard=#{shard.inspect} #{e.class}: #{e.message}\n#{e.backtrace.first(10).join("\n")}")
          end
        end

        iteration += 1
        # sweep_every_ticks <= 0 means "never sweep" (rather than crashing
        # the loop with ZeroDivisionError on `iteration % 0`).
        sweep_every = config.sweep_every_ticks.to_i
        sweep! if sweep_every.positive? && (iteration % sweep_every).zero?

        if admitted.zero?
          pause(config.idle_pause)
        else
          pause(config.busy_pause)
        end
      end
    end

    # sleep, but never with a negative argument (which would raise
    # ArgumentError mid-loop) — a non-positive pause just means "no pause".
    def pause(seconds)
      secs = seconds.to_f
      sleep(secs) if secs.positive?
    end

    def policy_names(filter)
      if filter
        [filter.to_s]
      else
        DispatchPolicy.registry.names
      end
    end

    def sweep!
      cfg = DispatchPolicy.config
      Repository.sweep_stale_inflight!(
        cutoff_seconds:        cfg.inflight_stale_after,
        queued_cutoff_seconds: cfg.inflight_queued_stale_after
      )
      sweep_inactive_partitions!(cfg)
      Repository.sweep_old_tick_samples!(cutoff_seconds: cfg.metrics_retention)
    rescue StandardError => e
      DispatchPolicy.config.logger&.error("[dispatch_policy] sweep error: #{e.class}: #{e.message}")
    end

    # One DELETE per registered policy plus one for the rest, rather than
    # a single global DELETE, because the right cutoff is per-policy: a
    # throttle's token bucket lives in the partition row's `gate_state`,
    # so collecting the row while its refill window is still running hands
    # that tenant a fresh quota. `rate: 2, per: 7.days` plus a day of
    # quiet used to mean four admits in one week. The cutoff for a
    # throttled policy is therefore at least its window.
    #
    # N+1 statements every `sweep_every_ticks` iterations, N = number of
    # registered policies. Both the per-policy and the catch-all DELETE
    # filter on the same indexed columns as before.
    def sweep_inactive_partitions!(cfg)
      default_cutoff = cfg.partition_inactive_after.to_i
      registered     = []

      DispatchPolicy.registry.each do |policy|
        registered << policy.name
        window = policy.static_throttle_window
        warn_unbounded_sweep(policy) if window.nil? && throttled?(policy)

        Repository.sweep_inactive_partitions!(
          cutoff_seconds: window ? [default_cutoff, window.ceil].max : default_cutoff,
          policy_name:    policy.name
        )
      end

      # Partitions whose policy this process doesn't know. Usually one
      # that was deleted from the code — but the registry is filled as a
      # side effect of job classes loading, so it is also every policy a
      # lazily-loaded process, a dashboard-only process or a half-rolled
      # deploy has not touched yet (ISSUES.md R3 records the same trap in
      # ManualAdmission). Without this pass a genuinely deleted policy's
      # rows would never be collected; with it on the normal cutoff, a
      # policy that is merely unloaded here has its token bucket deleted
      # and its tenants handed a fresh quota. So a row that still carries
      # a bucket waits out `unknown_policy_retention` instead — long
      # enough to cover any plausible window — while one with nothing to
      # lose goes on the usual cutoff.
      Repository.sweep_inactive_partitions!(
        cutoff_seconds:           default_cutoff,
        except_policies:          registered,
        throttled_cutoff_seconds: cfg.unknown_policy_retention
      )
    end

    def throttled?(policy)
      policy.gates.any? { |g| g.name == :throttle }
    end

    # Once per process per policy: a dynamic `per` can't be resolved
    # without a context, so the sweeper can only use the default cutoff.
    # Harmless while the window is shorter than partition_inactive_after,
    # which is the usual case — hence a warning, not an error.
    def warn_unbounded_sweep(policy)
      @warned_unbounded_sweep ||= {}
      return if @warned_unbounded_sweep[policy.name]

      @warned_unbounded_sweep[policy.name] = true
      DispatchPolicy.config.logger&.warn(
        "[dispatch_policy] policy #{policy.name.inspect} throttles with a dynamic `per`, so the " \
        "partition sweeper can't tell how long its token bucket takes to refill and falls back to " \
        "partition_inactive_after (#{DispatchPolicy.config.partition_inactive_after}s). If any " \
        "resolved window is longer than that, a swept partition starts again on a full bucket. " \
        "Use a static `per`, or raise partition_inactive_after above the longest window."
      )
    end
  end
end
