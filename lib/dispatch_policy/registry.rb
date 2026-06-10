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
      entry = @mutex.synchronize { @policies[name.to_s] }
      entry && entry[:policy]
    end

    def [](name)
      fetch(name)
    end

    def names
      @mutex.synchronize { @policies.keys }
    end

    def each(&block)
      # Snapshot under the lock, then iterate outside it: the block may run
      # arbitrary code (and Mutex isn't reentrant), so we must not hold the
      # lock while yielding.
      snapshot = @mutex.synchronize { @policies.values.map { |e| e[:policy] } }
      snapshot.each(&block)
    end

    def size
      @mutex.synchronize { @policies.size }
    end

    def clear
      @mutex.synchronize { @policies.clear }
    end
  end
end
