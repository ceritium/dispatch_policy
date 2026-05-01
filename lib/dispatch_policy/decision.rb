# frozen_string_literal: true

module DispatchPolicy
  class Decision
    attr_reader :allowed, :retry_after, :gate_state_patch, :reason

    def initialize(allowed:, retry_after: nil, gate_state_patch: nil, reason: nil)
      @allowed           = allowed
      @retry_after       = retry_after
      @gate_state_patch  = gate_state_patch
      @reason            = reason
    end

    def self.unlimited
      new(allowed: Float::INFINITY)
    end

    def self.deny(retry_after: nil, reason: nil)
      new(allowed: 0, retry_after: retry_after, reason: reason)
    end
  end
end
