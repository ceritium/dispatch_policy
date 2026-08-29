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
    #
    # Concurrency: `evaluate` reads the bucket OUTSIDE the admission
    # transaction, so two tick loops covering the same (policy, shard)
    # can both see a full bucket and both admit it — the same caveat
    # Gates::Concurrency documents for its COUNT(*). What they cannot do
    # is escape the cost: the bucket is settled inside the admission
    # UPDATE from the row's own value (Repository#record_partition_admit!),
    # so the two charges compose, the bucket goes negative, and the debt
    # comes out of the next window. The long-run rate holds; the burst is
    # transient. Run one tick loop per (policy, shard) — shard the policy
    # rather than duplicating loops on one shard — if you need the burst
    # gone too.
    class Throttle < Gate
      attr_reader :rate_proc, :per_proc

      # The refill window when it is a fixed number of seconds, nil when
      # `per` is a proc and can only be known per-context at admission
      # time. The partition sweeper reads this: the bucket lives in the
      # partition row's gate_state, so deleting the row inside a window
      # that is still being spent hands the tenant a fresh quota.
      attr_reader :static_per

      # Bucket size when `rate` is a fixed number, nil when it is a proc.
      # Paired with `static_refill_rate` it lets the sweeper work out what
      # the bucket holds RIGHT NOW — the stored value plus the refill
      # accrued since `refilled_at` — rather than trusting the stored
      # snapshot, which nothing refreshes while a partition sits idle.
      attr_reader :static_capacity

      # Tokens per second, when BOTH knobs are fixed. Not derivable from
      # `static_capacity`: a sub-unit rate floors the capacity at 1.0
      # while still refilling at the true `rate`, so `capacity / per`
      # would refill such a bucket twice as fast as the policy allows.
      attr_reader :static_refill_rate

      def initialize(rate:, per:)
        super()
        @rate_proc = rate.respond_to?(:call) ? rate : ->(_ctx) { rate }
        static_rate = rate.respond_to?(:call) ? nil : Float(rate || 0)
        @static_capacity = static_rate&.positive? ? [static_rate, 1.0].max : nil
        if per.respond_to?(:call)
          # Dynamic window (per-ctx), symmetric with a dynamic rate. Validated
          # per-evaluate since the value isn't known until admission time.
          @per_proc   = ->(ctx) { duration_seconds(per.call(ctx)) }
          @static_per = nil
        else
          fixed = duration_seconds(per)
          raise ArgumentError, "throttle :per must be > 0 (got #{fixed})" unless fixed.positive?
          @per_proc   = ->(_ctx) { fixed }
          @static_per = fixed
        end
        @static_refill_rate = @static_capacity && @static_per ? static_rate / @static_per : nil
      end

      def name
        :throttle
      end

      def evaluate(ctx, partition, admit_budget)
        per  = per_for(ctx)
        rate = rate_for(ctx)
        # rate <= 0 (e.g. a paused tenant) backs off for one window instead
        # of denying with a NULL retry_after. A NULL retry_after leaves the
        # partition immediately eligible, so it would be re-claimed and
        # re-evaluated every single tick — a busy-loop that also clobbers any
        # backoff a prior tick had set.
        return Decision.deny(retry_after: per, reason: "rate=0") if rate <= 0

        # The bucket holds at least one whole token; otherwise a sub-unit rate
        # (e.g. rate: 0.5) could never accumulate a full token and would never
        # admit. refill_rate stays at the true `rate` so the long-run pace is
        # exact — the floor only sets the burst ceiling.
        capacity    = [rate, 1.0].max
        refill_rate = rate / per
        state       = (partition["gate_state"] || {})["throttle"] || {}
        tokens      = (state["tokens"] || capacity).to_f
        refilled_at = (state["refilled_at"] || now).to_f

        elapsed     = [now - refilled_at, 0.0].max
        tokens      = [tokens + (elapsed * refill_rate), capacity].min

        # Nothing is written from here. The refill above is a pure
        # function of the stored `refilled_at` and the clock, so
        # persisting it buys nothing — and persisting it on the DENY path
        # actively hurt: a deny landing after a concurrent admission
        # overwrote the charged bucket with an uncharged refill, undoing
        # the admission's cost. What the bucket owes is settled in the
        # admission UPDATE instead, from the row's own value; `charge`
        # carries the numbers that needs. `tokens` rides along so #consume
        # can mirror the result in memory for the tick's second pass.
        #
        # `now` travels with it because the bucket must be read and
        # written on ONE clock. The charge recomputes the refill in SQL,
        # but from THIS timestamp — not from Postgres `now()`. Mixing the
        # two means the app clock refills a bucket the database clock
        # stamped: an offset O between them silently adds O * refill_rate
        # phantom tokens to every evaluate, and `now()` is the
        # TRANSACTION timestamp, so an enclosing transaction (Rails
        # transactional tests, a host wrapping the tick) freezes it while
        # `config.now` keeps moving.
        charge = { capacity:    capacity,
                   refill_rate: refill_rate,
                   tokens:      tokens,
                   now:         now }

        # Under one whole token, not `floor == 0`: the bucket can be
        # NEGATIVE now that a concurrent over-admission is repaid rather
        # than forgiven, and a debt is even less admissible than an empty
        # bucket. `missing` is then > 1 and the backoff covers the debt.
        if tokens < 1.0
          missing = 1.0 - tokens
          return Decision.new(allowed: 0,
                              retry_after: missing / refill_rate,
                              reason: "throttle_empty")
        end

        allowed = [tokens.floor, admit_budget].min
        Decision.new(allowed: allowed, charge: charge)
      end

      # Settles the bucket against the number of jobs actually admitted.
      # `evaluate` recorded the post-refill token count in the decision's
      # patch; here we subtract exactly `admitted_count` (≤ allowed), so
      # the bucket is charged for jobs that really left, never for unspent
      # budget. Called by Pipeline.settle after the claim.
      # The persisted value is computed in SQL (see the `charge` above);
      # what this returns is the in-memory mirror the Tick applies to the
      # partition it is holding, so the second admission pass in the same
      # tick evaluates against a bucket that already reflects the first.
      # Without it pass-2 would re-read the pre-admission count and hand
      # out the same tokens twice inside one tick.
      def consume(decision, admitted_count)
        c = decision.charge
        return nil unless c

        { "throttle" => { "tokens"      => c[:tokens] - admitted_count,
                          "refilled_at" => c[:now] } }
      end

      private

      def per_for(ctx)
        value = @per_proc.call(ctx)
        raise ArgumentError, "throttle :per must be > 0 (got #{value})" unless value.positive?
        value
      end

      def rate_for(ctx)
        value = @rate_proc.call(ctx)
        # Float, not Integer: a fractional rate (e.g. 2.5/sec) must keep its
        # fractional part or the bucket systematically under-admits by
        # truncating every refill. nil means "no rate configured" → deny.
        value.nil? ? 0.0 : Float(value)
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
