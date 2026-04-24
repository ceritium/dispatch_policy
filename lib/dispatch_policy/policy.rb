# frozen_string_literal: true

module DispatchPolicy
  class Policy
    attr_reader :job_class, :gates, :snapshots, :dedupe_key_builder

    def initialize(job_class, &block)
      @job_class           = job_class
      @name                = job_class.name.underscore.tr("/", "-")
      @context_builder     = ->(_args) { {} }
      @gates               = []
      @snapshots           = {}
      @dedupe_key_builder  = nil
      @round_robin_builder = nil
      instance_eval(&block) if block
      DispatchPolicy.registry[@name] = job_class
    end

    def name(value = nil)
      return @name if value.nil?
      DispatchPolicy.registry.delete(@name)
      @name = value.to_s
      DispatchPolicy.registry[@name] = @job_class
    end

    def context(builder)
      @context_builder = builder
    end

    def context_builder
      @context_builder
    end

    def snapshot(key, builder)
      @snapshots[key.to_sym] = builder
    end

    def dedupe_key(builder)
      @dedupe_key_builder = builder
    end

    def build_dedupe_key(arguments)
      return nil unless @dedupe_key_builder
      key = @dedupe_key_builder.call(arguments)
      key&.to_s
    end

    def round_robin_by(builder)
      @round_robin_builder = builder
    end

    def round_robin?
      !@round_robin_builder.nil?
    end

    def build_round_robin_key(arguments)
      return nil unless @round_robin_builder
      key = @round_robin_builder.call(arguments)
      key.nil? || key.to_s.empty? ? nil : key.to_s
    end

    def gate(type, **opts)
      gate_class = DispatchPolicy::Gate.registry.fetch(type.to_sym) do
        raise ArgumentError, "Unknown gate: #{type}. Known: #{DispatchPolicy::Gate.registry.keys}"
      end
      @gates << gate_class.new(policy: self, name: type.to_sym, **opts)
    end

    def build_snapshot(arguments)
      @snapshots.transform_values { |builder| builder.call(arguments) }
    end
  end
end
