# frozen_string_literal: true

module DispatchPolicy
  class Decision
    # `charge` describes state the gate wants settled ATOMICALLY, in the
    # same UPDATE as the admission, from the row's own current value —
    # rather than as a literal `gate_state_patch` computed in Ruby from a
    # read that already happened. A patch written that way is a
    # read-modify-write: two ticks racing on one partition overwrite each
    # other and one of the two admissions goes uncharged. Today only the
    # throttle uses it; see Repository.record_partition_admit!.
    attr_reader :allowed, :retry_after, :gate_state_patch, :reason, :charge

    def initialize(allowed:, retry_after: nil, gate_state_patch: nil, reason: nil, charge: nil)
      @allowed           = allowed
      @retry_after       = retry_after
      @gate_state_patch  = gate_state_patch
      @reason            = reason
      @charge            = charge
    end

    def self.unlimited
      new(allowed: Float::INFINITY)
    end

    def self.deny(retry_after: nil, reason: nil)
      new(allowed: 0, retry_after: retry_after, reason: reason)
    end
  end
end
