# frozen_string_literal: true

module DispatchPolicy
  class Gate
    def name
      raise NotImplementedError
    end

    # @param ctx [DispatchPolicy::Context]
    # @param partition [Hash] the partitions row (string keys)
    # @param admit_budget [Integer] the budget remaining from earlier gates
    # @return [DispatchPolicy::Decision]
    def evaluate(_ctx, _partition, _admit_budget)
      raise NotImplementedError
    end

    # Called after a successful admit to update gate-local state.
    # Returns a hash patch to merge into partition.gate_state, or nil.
    def consume(_decision, _admitted_count); nil; end
  end
end
