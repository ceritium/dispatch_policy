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
      @round_robin_weight  = :equal
      @round_robin_window  = 60
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

    def round_robin_by(builder, weight: :equal, window: 60)
      raise ArgumentError, "weight must be :equal or :time" unless %i[equal time].include?(weight)
      @round_robin_builder = builder
      @round_robin_weight  = weight
      @round_robin_window  = window
    end

    def round_robin?
      !@round_robin_builder.nil?
    end

    def round_robin_weight
      @round_robin_weight
    end

    def round_robin_window
      @round_robin_window
    end

    def build_round_robin_key(arguments)
      return nil unless @round_robin_builder
      key = @round_robin_builder.call(arguments)
      return nil if key.nil? || key.to_s.empty?
      key.to_s[0, DispatchPolicy::MAX_PARTITION_KEY_LENGTH]
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
