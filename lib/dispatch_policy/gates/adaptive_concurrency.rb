# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Self-tuning concurrency gate. Like :concurrency but with a
    # per-partition cap (`current_max`) that grows when the adapter
    # queue is empty and shrinks when it builds up. AIMD loop persisted
    # in `dispatch_policy_adaptive_concurrency_stats`.
    #
    # Feedback signal is `queue_lag_ms = perform_start - admitted_at`
    # (time the job spent waiting in the adapter after admission).
    # Pure saturation signal — slow performs in the downstream service
    # don't punish admissions if workers still drain the queue quickly.
    #
    # Update rule applied after each perform (in InflightTracker.track):
    #
    #   succeeded? & ewma_lag <= target_lag_ms → current_max += 1
    #   succeeded? & ewma_lag >  target_lag_ms → current_max *= slow_factor
    #   failed?                                → current_max *= fail_factor
    #
    # Always clamped to >= min. Never grows without bound — the
    # algorithm self-limits via target_lag_ms.
    class AdaptiveConcurrency < Gate
      DEFAULT_FULL_BACKOFF = 1.0  # seconds
      DEFAULT_EWMA_ALPHA   = 0.5  # weight of the new sample in the EWMA
      DEFAULT_FAIL_FACTOR  = 0.5  # halve on perform raise
      DEFAULT_SLOW_FACTOR  = 0.95 # gentle shrink on overload

      attr_reader :initial_max, :target_lag_ms, :min,
                  :ewma_alpha, :fail_factor, :slow_factor, :full_backoff

      def initialize(initial_max:, target_lag_ms:, min: 1,
                     ewma_alpha: DEFAULT_EWMA_ALPHA,
                     failure_decrease_factor: DEFAULT_FAIL_FACTOR,
                     overload_decrease_factor: DEFAULT_SLOW_FACTOR,
                     full_backoff: DEFAULT_FULL_BACKOFF)
        super()
        @initial_max   = Integer(initial_max)
        @target_lag_ms = Float(target_lag_ms)
        @min           = Integer(min)
        @ewma_alpha    = Float(ewma_alpha)
        @fail_factor   = Float(failure_decrease_factor)
        @slow_factor   = Float(overload_decrease_factor)
        @full_backoff  = Float(full_backoff)
        raise ArgumentError, "target_lag_ms must be > 0" unless @target_lag_ms.positive?
        raise ArgumentError, "min must be >= 1"          unless @min >= 1
        raise ArgumentError, "initial_max must be >= min" unless @initial_max >= @min
      end

      def name
        :adaptive_concurrency
      end

      def evaluate(ctx, partition, admit_budget)
        policy_name = partition["policy_name"]
        key         = inflight_partition_key(policy_name, ctx)

        # Seed lazily so the very first admission has a row to read
        # (and so record_observation can UPDATE without a check).
        Repository.adaptive_seed!(
          policy_name:   policy_name,
          partition_key: key,
          initial_max:   @initial_max
        )

        cap = Repository.adaptive_current_max(
          policy_name:   policy_name,
          partition_key: key
        ) || @initial_max
        cap = [cap, @min].max

        in_flight = Repository.count_inflight(
          policy_name:   policy_name,
          partition_key: key
        )
        remaining = cap - in_flight

        # Safety valve. AIMD can shrink current_max during a slow burst;
        # if the partition then idles, no observations come in to grow
        # the cap back. When in_flight == 0 we ensure at least
        # initial_max so the partition never fossilizes at min.
        remaining = [remaining, @initial_max].max if in_flight.zero?

        if remaining <= 0
          return Decision.new(allowed: 0,
                              retry_after: @full_backoff,
                              reason: "adaptive_concurrency_full")
        end

        Decision.new(allowed: [remaining, admit_budget].min)
      end

      # Same canonical scope as the staged_jobs partition_key — every
      # gate in a policy uses `policy.partition_for(ctx)` so the
      # inflight count and the adaptive stats line up exactly.
      def inflight_partition_key(policy_name, ctx)
        policy = DispatchPolicy.registry.fetch(policy_name)
        raise InvalidPolicy, "unknown policy #{policy_name.inspect}" unless policy
        policy.partition_for(ctx)
      end

      # Called from InflightTracker.track after each perform completes
      # (success or failure). Updates the AIMD state atomically in one
      # SQL statement.
      def record_observation(policy_name:, partition_key:, queue_lag_ms:, succeeded:)
        Repository.adaptive_seed!(
          policy_name:   policy_name,
          partition_key: partition_key.to_s,
          initial_max:   @initial_max
        )
        Repository.adaptive_record!(
          policy_name:   policy_name,
          partition_key: partition_key.to_s,
          queue_lag_ms:  queue_lag_ms,
          succeeded:     succeeded,
          alpha:         @ewma_alpha,
          target_lag_ms: @target_lag_ms,
          fail_factor:   @fail_factor,
          slow_factor:   @slow_factor,
          min:           @min
        )
      end
    end
  end
end
