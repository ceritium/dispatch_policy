# frozen_string_literal: true

module DispatchPolicy
  # Composes a sequence of gates into a single admission decision for one
  # partition. Returns a value object describing how many jobs may be
  # admitted right now and which gate-state patches to persist.
  class Pipeline
    Result = Struct.new(:admit_count, :retry_after, :gate_state_patch, :reasons, keyword_init: true)

    def initialize(policy)
      @policy = policy
    end

    def call(ctx, partition, max_budget)
      budget          = max_budget
      retry_after     = nil
      patch           = {}
      reasons         = []
      decisions       = []

      @policy.gates.each do |gate|
        decision = gate.evaluate(ctx, partition, budget)
        decisions << [gate, decision]
        budget   = decision.allowed.finite? ? [budget, decision.allowed].min : budget
        if decision.retry_after
          retry_after = retry_after.nil? ? decision.retry_after : [retry_after, decision.retry_after].min
        end
        reasons << "#{gate.name}:#{decision.reason}" if decision.reason
        break if budget.zero?
      end

      admit_count = budget.finite? ? budget : max_budget
      admit_count = 0 if admit_count.negative?

      decisions.each do |_, decision|
        next unless decision.gate_state_patch
        patch.merge!(decision.gate_state_patch)
      end

      Result.new(
        admit_count:       admit_count,
        retry_after:       retry_after,
        gate_state_patch:  patch,
        reasons:           reasons
      )
    end
  end
end
