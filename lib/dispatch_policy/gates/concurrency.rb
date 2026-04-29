# frozen_string_literal: true

module DispatchPolicy
  module Gates
    class Concurrency < Gate
      attr_reader :max

      def configure(max:)
        @max = max
      end

      def tracks_inflight?
        true
      end

      def filter(batch, context)
        by_partition = batch.group_by { |staged| partition_key_for(context.for(staged)) }

        in_flight = PartitionInflightCount.fetch_many(
          policy_name:    policy.name,
          gate_name:      name,
          partition_keys: by_partition.keys
        )

        admitted = []
        by_partition.each do |partition_key, jobs|
          jobs.each do |staged|
            ctx   = context.for(staged)
            limit = resolve(@max, ctx).to_i
            used  = in_flight.fetch(partition_key, 0)
            if used < limit
              admitted << [ staged, partition_key ]
              in_flight[partition_key] = used + 1
            end
          end
        end

        context.record_partitions(admitted, gate: name)
        admitted.map(&:first)
      end
    end

    Gate.register(:concurrency, Concurrency)
  end
end
