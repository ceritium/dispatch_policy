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
      attr_reader :max_proc

      def initialize(max:, partition_by: nil)
        super(partition_by: partition_by)
        @max_proc = max.respond_to?(:call) ? max : ->(_ctx) { max }
      end

      def name
        :concurrency
      end

      def evaluate(ctx, partition, admit_budget)
        cap = capacity_for(ctx)
        return Decision.deny(reason: "max=0") if cap <= 0

        in_flight = Repository.count_inflight(
          policy_name:   partition["policy_name"],
          partition_key: inflight_partition_key(partition["policy_name"], ctx)
        )
        remaining = cap - in_flight
        return Decision.new(allowed: 0, reason: "concurrency_full") if remaining <= 0

        Decision.new(allowed: [remaining, admit_budget].min)
      end

      # Stable key used by InflightTracker (write) and #evaluate (read).
      # Includes the gate name so multiple gates of the same name (future)
      # could coexist; for now there is only one concurrency gate per policy.
      def inflight_partition_key(_policy_name, ctx)
        "concurrency=#{partition_for(ctx)}"
      end

      private

      def capacity_for(ctx)
        value = @max_proc.call(ctx)
        value.nil? ? 0 : Integer(value)
      end
    end
  end
end
