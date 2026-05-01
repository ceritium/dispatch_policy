# frozen_string_literal: true

module DispatchPolicy
  # Thread-local guard. When active, ActiveJob#enqueue calls within the block
  # bypass the dispatch_policy around_enqueue and reach the real adapter.
  module Bypass
    KEY = :__dispatch_policy_bypass__

    module_function

    def with
      previous = Thread.current[KEY]
      Thread.current[KEY] = true
      yield
    ensure
      Thread.current[KEY] = previous
    end

    def active?
      Thread.current[KEY] == true
    end
  end
end
