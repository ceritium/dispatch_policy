# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Per-partition compute-time budget. The bucket holds milliseconds (not
    # request count); each completed perform debits its actual duration.
    # Slow downstreams self-throttle naturally — a webhook that takes 10×
    # longer admits 10× fewer requests for the same budget.
    #
    # Reuses the ThrottleBucket schema. The `tokens` column stores ms
    # remaining in the current window; refill rate is budget_ms / per.
    # Admission only checks `tokens > 0`; the actual debit is applied
    # post-perform via #record_completion (called from
    # Dispatchable#around_perform), so a slow perform may take the bucket
    # negative and recovery is paced by the natural refill — that's the
    # backpressure mechanism.
    class TimeBudget < Gate
      def configure(budget_ms:, per: 60)
        @budget_ms = budget_ms
        @per       = per
      end

      def tracks_inflight?
        false
      end

      attr_reader :budget_ms, :per

      def filter(batch, context)
        by_partition = batch.group_by { |staged| partition_key_for(context.for(staged)) }

        admitted = []
        by_partition.each do |partition_key, jobs|
          sample_ctx = context.for(jobs.first)
          budget     = resolve(@budget_ms, sample_ctx).to_f
          per        = resolve(@per, sample_ctx).to_f

          bucket = ThrottleBucket.lock(
            policy_name:   policy.name,
            gate_name:     name,
            partition_key: partition_key,
            burst:         budget
          )
          # Refill: budget_ms tokens per `per` seconds. Cap is the budget itself.
          bucket.refill!(rate: budget, per: per, burst: budget)

          # Admit while we still have positive budget. Don't decrement here:
          # debit happens in record_completion with the actual duration.
          if bucket.tokens.positive?
            jobs.each { |staged| admitted << [ staged, partition_key ] }
          end
          bucket.save!
        end

        context.record_partitions(admitted, gate: name)
        admitted.map(&:first)
      end

      # Called from Dispatchable#around_perform when the perform finishes.
      # Debits the actual duration from the partition's bucket. Tokens may
      # go negative — that's by design; refill brings the bucket back to
      # positive over time and admission resumes.
      def record_completion(partition_key:, duration_ms:)
        ThrottleBucket.debit_ms!(
          policy_name:   policy.name,
          gate_name:     name,
          partition_key: partition_key.to_s,
          ms:            duration_ms.to_f
        )
      end
    end

    Gate.register(:time_budget, TimeBudget)
  end
end
