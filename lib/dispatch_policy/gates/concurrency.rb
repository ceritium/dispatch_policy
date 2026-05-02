# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Concurrency gate: caps in-flight jobs per partition.
    #
    # The partition scope is the policy's `partition_by`. Inflight rows
    # are written by InflightTracker around_perform with the same key,
    # so this gate's COUNT(*) aggregates the same canonical scope as
    # the staged_jobs row.
    class Concurrency < Gate
      DEFAULT_FULL_BACKOFF = 1.0 # seconds

      attr_reader :max_proc, :full_backoff

      def initialize(max:, full_backoff: DEFAULT_FULL_BACKOFF)
        super()
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

      # The inflight key is always the policy's canonical partition
      # value — same as what's stored in staged_jobs.partition_key.
      # This is what makes throttle + concurrency in the same policy
      # enforce their state at exactly one consistent scope.
      def inflight_partition_key(policy_name, ctx)
        policy = DispatchPolicy.registry.fetch(policy_name)
        raise InvalidPolicy, "unknown policy #{policy_name.inspect}" unless policy
        policy.partition_for(ctx)
      end

      private

      def capacity_for(ctx)
        value = @max_proc.call(ctx)
        value.nil? ? 0 : Integer(value)
      end
    end
  end
end
