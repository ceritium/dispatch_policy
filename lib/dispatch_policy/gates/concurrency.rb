# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Concurrency gate: caps in-flight jobs per partition.
    #
    # The gate's `partition_by` may be coarser than the staged-job
    # partition_key (e.g. concurrency partitions by account while throttle
    # partitions by endpoint). All staged partitions whose ctx maps to the
    # same `partition_for(ctx)` therefore share a budget — they all see
    # the same in-flight count when evaluated.
    #
    # Inflight rows are keyed by `inflight_partition_key(policy, ctx)`,
    # written by InflightTracker around_perform and read by this gate.
    class Concurrency < Gate
      DEFAULT_FULL_BACKOFF = 1.0 # seconds

      attr_reader :max_proc, :full_backoff

      def initialize(max:, partition_by: nil, full_backoff: DEFAULT_FULL_BACKOFF)
        super(partition_by: partition_by)
        @max_proc     = max.respond_to?(:call) ? max : ->(_ctx) { max }
        @full_backoff = full_backoff.to_f
      end

      def name
        :concurrency
      end

      def evaluate(ctx, partition, admit_budget)
        cap = capacity_for(ctx)
        return Decision.deny(retry_after: @full_backoff, reason: "max=0") if cap <= 0

        in_flight = Repository.count_inflight(
          policy_name:   partition["policy_name"],
          partition_key: inflight_partition_key(partition["policy_name"], ctx)
        )
        remaining = cap - in_flight
        if remaining <= 0
          # Stop hammering this partition with COUNT(*) every tick — back off
          # until enough jobs are likely to have finished.
          return Decision.new(allowed: 0, retry_after: @full_backoff, reason: "concurrency_full")
        end

        Decision.new(allowed: [remaining, admit_budget].min)
      end

      # Stable key used by InflightTracker (write) and #evaluate (read).
      #
      # When the policy declares its own `partition_by` (the recommended
      # form), the inflight key matches the staged_jobs partition_key
      # exactly — same canonical scope for all gates, no dilution.
      #
      # Without a policy-level partition_by, falls back to the gate's
      # own partition_for(ctx) prefixed with the gate name. This keeps
      # the legacy per-gate `partition_by:` working: inflight is keyed
      # by `"concurrency=<account>"` independent of the staged
      # partition_key, so concurrency caps are still enforced at the
      # gate's intended scope.
      def inflight_partition_key(policy_name, ctx)
        policy = DispatchPolicy.registry.fetch(policy_name)
        if policy&.partition_by_proc
          policy.partition_for(ctx)
        else
          "concurrency=#{partition_for(ctx)}"
        end
      end

      private

      def capacity_for(ctx)
        value = @max_proc.call(ctx)
        value.nil? ? 0 : Integer(value)
      end
    end
  end
end
