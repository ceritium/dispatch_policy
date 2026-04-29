# frozen_string_literal: true

module DispatchPolicy
  module Gates
    class GlobalCap < Gate
      attr_reader :max

      def configure(max:)
        @max = max
      end

      def tracks_inflight?
        true
      end

      def filter(batch, context)
        limit     = resolve(@max, nil).to_i
        in_flight = PartitionInflightCount.total_for(policy_name: policy.name, gate_name: name)
        capacity  = [ limit - in_flight, 0 ].max
        head      = batch.first(capacity)
        context.record_partitions(head.map { |s| [ s, "default" ] }, gate: name)
        head
      end
    end

    Gate.register(:global_cap, GlobalCap)
  end
end
