# frozen_string_literal: true

module DispatchPolicy
  class PolicyDSL
    GATE_TYPES = {
      throttle:    Gates::Throttle,
      concurrency: Gates::Concurrency
    }.freeze

    def self.build(name, &block)
      dsl = new(name)
      dsl.instance_eval(&block) if block
      dsl.to_policy
    end

    def initialize(name)
      @name                 = name
      @context_proc         = nil
      @gates                = []
      @retry_strategy       = :restage
      @queue_name           = nil
      @admission_batch_size = nil
    end

    def context(callable = nil, &block)
      @context_proc = callable || block
    end

    def gate(type, **options)
      klass = GATE_TYPES[type] || raise(UnknownGate, "unknown gate type: #{type.inspect}")
      @gates << klass.new(**options)
    end

    def retry_strategy(strategy)
      @retry_strategy = strategy
    end

    def queue_name(name)
      @queue_name = name.to_s
    end

    def admission_batch_size(size)
      @admission_batch_size = Integer(size)
    end

    def to_policy
      Policy.new(
        name:                 @name,
        context_proc:         @context_proc,
        gates:                @gates,
        retry_strategy:       @retry_strategy,
        queue_name:           @queue_name,
        admission_batch_size: @admission_batch_size
      )
    end
  end
end
