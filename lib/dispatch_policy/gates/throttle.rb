# frozen_string_literal: true

module DispatchPolicy
  module Gates
    class Throttle < Gate
      def configure(rate:, per:, burst: nil)
        @rate  = rate
        @per   = per
        @burst = burst
      end

      # Consumed tokens refill over time, no release step.
      def tracks_inflight?
        false
      end

      def filter(batch, context)
        by_partition = batch.group_by { |staged| partition_key_for(context.for(staged)) }

        admitted = []
        # Sort keys before acquiring per-partition row locks: two ticks
        # processing overlapping partitions in different group_by orders
        # would otherwise deadlock on each other's FOR UPDATE rows.
        by_partition.keys.sort.each do |partition_key|
          jobs       = by_partition[partition_key]
          sample_ctx = context.for(jobs.first)
          rate       = resolve(@rate, sample_ctx).to_f
          per        = @per.to_f
          burst      = (resolve(@burst, sample_ctx) || rate).to_f

          bucket = ThrottleBucket.lock(
            policy_name:   policy.name,
            gate_name:     name,
            partition_key: partition_key,
            burst:         burst
          )
          bucket.refill!(rate: rate, per: per, burst: burst)

          jobs.each do |staged|
            if bucket.consume(1)
              admitted << [ staged, partition_key ]
            else
              break
            end
          end
          bucket.save!
        end

        context.record_partitions(admitted, gate: name)
        admitted.map(&:first)
      end
    end

    Gate.register(:throttle, Throttle)
  end
end
