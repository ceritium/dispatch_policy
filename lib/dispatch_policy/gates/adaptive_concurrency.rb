# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Adaptive variant of :concurrency. The cap per partition (current_max)
    # shrinks when recent performs are slow or fail, grows back when they
    # succeed under target_latency. AIMD loop on a per-partition stats
    # row; the underlying in-flight counter is the same PartitionInflightCount
    # used by :concurrency, so admissions and releases stay cheap.
    class AdaptiveConcurrency < Gate
      DEFAULT_EWMA_ALPHA  = 0.2
      DEFAULT_FAIL_FACTOR = 0.5
      DEFAULT_SLOW_FACTOR = 0.9
      # Large default ceiling so forgetting `max:` doesn't accidentally
      # uncap anything; users who want a tighter safety net set their own.
      DEFAULT_MAX         = 1_000

      def configure(initial_max:, target_latency:,
                    min: 1,
                    max: DEFAULT_MAX,
                    ewma_alpha: DEFAULT_EWMA_ALPHA,
                    failure_decrease_factor: DEFAULT_FAIL_FACTOR,
                    overload_decrease_factor: DEFAULT_SLOW_FACTOR)
        @initial_max    = initial_max
        @min            = min
        @max            = max
        @target_latency = target_latency
        @ewma_alpha     = ewma_alpha
        @fail_factor    = failure_decrease_factor
        @slow_factor    = overload_decrease_factor
      end

      def tracks_inflight?
        true
      end

      attr_reader :initial_max, :min, :max, :target_latency,
                  :ewma_alpha, :fail_factor, :slow_factor

      def filter(batch, context)
        by_partition = batch.group_by { |staged| partition_key_for(context.for(staged)) }

        # Seed any missing stats rows so the first admission has something
        # to read. Cheap: one INSERT ... ON CONFLICT DO NOTHING per key.
        by_partition.each_key do |key|
          AdaptiveConcurrencyStats.seed!(
            policy_name:   policy.name,
            gate_name:     name,
            partition_key: key,
            initial_max:   resolve(@initial_max, nil).to_i
          )
        end

        stats = AdaptiveConcurrencyStats.fetch_many(
          policy_name:    policy.name,
          gate_name:      name,
          partition_keys: by_partition.keys
        )

        in_flight = PartitionInflightCount.fetch_many(
          policy_name:    policy.name,
          gate_name:      name,
          partition_keys: by_partition.keys
        )

        min_v = resolve(@min, nil).to_i
        max_v = resolve(@max, nil).to_i

        admitted = []
        by_partition.each do |partition_key, jobs|
          effective_max = stats.dig(partition_key, :current_max) || resolve(@initial_max, nil).to_i
          effective_max = effective_max.clamp(min_v, max_v)
          used = in_flight.fetch(partition_key, 0)

          jobs.each do |staged|
            break unless used < effective_max
            admitted << [ staged, partition_key ]
            used += 1
          end
        end

        context.record_partitions(admitted, gate: name)
        admitted.map(&:first)
      end

      # Called by Dispatchable#around_perform for each adaptive gate that
      # touched this job. Lives on the gate instance because configuration
      # (alpha, target_latency, etc.) is per gate.
      def record_observation(partition_key:, duration_ms:, succeeded:)
        AdaptiveConcurrencyStats.record_observation!(
          policy_name:       policy.name,
          gate_name:         name,
          partition_key:     partition_key.to_s,
          duration_ms:       duration_ms,
          succeeded:         succeeded,
          alpha:             @ewma_alpha,
          min:               resolve(@min, nil).to_i,
          max:               resolve(@max, nil).to_i,
          target_latency_ms: resolve(@target_latency, nil).to_f,
          fail_factor:       @fail_factor,
          slow_factor:       @slow_factor,
          initial_max:       resolve(@initial_max, nil).to_i
        )
      end
    end

    Gate.register(:adaptive_concurrency, AdaptiveConcurrency)
  end
end
