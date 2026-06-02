# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Token bucket throttle gate.
    #
    # Persists state in partitions.gate_state["throttle"] = {
    #   "tokens"      => Float,   # current tokens, capped at bucket size
    #   "refilled_at" => Float    # epoch seconds, last refill
    # }
    #
    # The partition scope this gate enforces against is the policy's
    # `partition_by` (declared in the policy DSL block, not on the gate).
    # The bucket lives on the staged partition row — one row per
    # `policy.partition_for(ctx)` value, one bucket per row, no dilution.
    class Throttle < Gate
      attr_reader :rate_proc, :per

      def initialize(rate:, per:)
        super()
        @rate_proc = rate.respond_to?(:call) ? rate : ->(_ctx) { rate }
        @per       = duration_seconds(per)
        raise ArgumentError, "throttle :per must be > 0 (got #{@per})" unless @per.positive?
      end

      def name
        :throttle
      end

      def evaluate(ctx, partition, admit_budget)
        capacity = capacity_for(ctx)
        return Decision.deny(reason: "rate=0") if capacity <= 0

        refill_rate = capacity.to_f / @per
        state       = (partition["gate_state"] || {})["throttle"] || {}
        tokens      = (state["tokens"] || capacity).to_f
        refilled_at = (state["refilled_at"] || now).to_f

        elapsed     = [now - refilled_at, 0.0].max
        tokens      = [tokens + (elapsed * refill_rate), capacity.to_f].min

        # The patch records the post-refill bucket WITHOUT deducting yet.
        # The actual deduction is deferred to #consume, which runs once
        # the admission TX knows how many staged rows were really claimed.
        # Deducting `allowed` here over-charges the bucket whenever fewer
        # jobs are admitted than allowed — a later gate capping admit_count,
        # future-scheduled rows skipped by the `scheduled_at <= now()`
        # filter, or rows another tick grabbed under SKIP LOCKED.
        patch = { "tokens" => tokens, "refilled_at" => now }

        whole = tokens.floor
        if whole.zero?
          missing      = 1.0 - tokens
          retry_after  = missing / refill_rate
          return Decision.new(allowed: 0,
                              retry_after: retry_after,
                              gate_state_patch: { "throttle" => patch },
                              reason: "throttle_empty")
        end

        allowed = [whole, admit_budget].min
        Decision.new(allowed: allowed, gate_state_patch: { "throttle" => patch })
      end

      # Settles the bucket against the number of jobs actually admitted.
      # `evaluate` recorded the post-refill token count in the decision's
      # patch; here we subtract exactly `admitted_count` (≤ allowed), so
      # the bucket is charged for jobs that really left, never for unspent
      # budget. Called by Pipeline.settle after the claim.
      def consume(decision, admitted_count)
        st = decision.gate_state_patch && decision.gate_state_patch["throttle"]
        return nil unless st

        { "throttle" => { "tokens"      => st["tokens"].to_f - admitted_count,
                          "refilled_at" => st["refilled_at"] } }
      end

      private

      def capacity_for(ctx)
        value = @rate_proc.call(ctx)
        value.nil? ? 0 : Integer(value)
      end

      def now
        DispatchPolicy.config.now.to_f
      end

      def duration_seconds(value)
        if value.is_a?(Numeric)
          value.to_f
        elsif value.respond_to?(:to_f) && value.respond_to?(:seconds)
          value.to_f
        else
          raise ArgumentError, "throttle :per must be a numeric duration in seconds (got #{value.inspect})"
        end
      end
    end
  end
end
