# frozen_string_literal: true

module DispatchPolicy
  class Registry
    def initialize
      @mutex    = Mutex.new
      @policies = {}
    end

    def register(policy, owner: nil)
      @mutex.synchronize do
        existing = @policies[policy.name]
        if existing && existing[:owner] != owner
          raise PolicyAlreadyRegistered, "policy #{policy.name.inspect} already registered for #{existing[:owner]}"
        end
        @policies[policy.name] = { policy: policy, owner: owner }
      end
      policy
    end

    def fetch(name)
      entry = @policies[name.to_s]
      entry && entry[:policy]
    end

    def [](name)
      fetch(name)
    end

    def names
      @policies.keys
    end

    def each(&block)
      @policies.values.map { |e| e[:policy] }.each(&block)
    end

    def size
      @policies.size
    end

    def clear
      @mutex.synchronize { @policies.clear }
    end
  end
end
