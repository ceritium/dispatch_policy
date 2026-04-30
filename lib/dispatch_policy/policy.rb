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
      # Per-policy config overrides. nil means "fall back to
      # DispatchPolicy.config.X". Set inside the dispatch_policy block
      # via the DSL methods of the same name (without override_).
      @override_batch_size                          = nil
      @override_round_robin_quantum                 = nil
      @override_round_robin_max_partitions_per_tick = nil
      @override_lease_duration                      = nil
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
      key.nil? || key.to_s.empty? ? nil : key.to_s
    end

    # Per-policy config overrides — DSL inside the dispatch_policy block.
    # Each setter accepts a value; the matching effective_X reader
    # returns the override or falls back to DispatchPolicy.config.

    def batch_size(value = NIL_DEFAULT)
      return effective_batch_size if value.equal?(NIL_DEFAULT)
      @override_batch_size = value
    end

    def round_robin_quantum(value = NIL_DEFAULT)
      return effective_round_robin_quantum if value.equal?(NIL_DEFAULT)
      @override_round_robin_quantum = value
    end

    def round_robin_max_partitions_per_tick(value = NIL_DEFAULT)
      return effective_round_robin_max_partitions_per_tick if value.equal?(NIL_DEFAULT)
      @override_round_robin_max_partitions_per_tick = value
    end

    def lease_duration(value = NIL_DEFAULT)
      return effective_lease_duration if value.equal?(NIL_DEFAULT)
      @override_lease_duration = value
    end

    def effective_batch_size
      @override_batch_size || DispatchPolicy.config.batch_size
    end

    def effective_round_robin_quantum
      @override_round_robin_quantum || DispatchPolicy.config.round_robin_quantum
    end

    def effective_round_robin_max_partitions_per_tick
      @override_round_robin_max_partitions_per_tick ||
        DispatchPolicy.config.round_robin_max_partitions_per_tick ||
        effective_batch_size
    end

    def effective_lease_duration
      @override_lease_duration || DispatchPolicy.config.lease_duration
    end

    # Reports which knobs are overridden vs inheriting global defaults.
    # Useful for Stats and operator visibility.
    def config_overrides
      {
        batch_size:                          @override_batch_size,
        round_robin_quantum:                 @override_round_robin_quantum,
        round_robin_max_partitions_per_tick: @override_round_robin_max_partitions_per_tick,
        lease_duration:                      @override_lease_duration
      }.compact
    end

    NIL_DEFAULT = Object.new.freeze
    private_constant :NIL_DEFAULT

    # Reload @override_X ivars from the dispatch_policy_policy_configs
    # table. Called by TickLoop at startup so cluster-wide tunes
    # propagate within tick_max_duration without a redeploy.
    #
    # Behavior depends on DispatchPolicy.config.policy_config_source:
    #   :db   — DB rows overwrite the in-memory DSL values.
    #   :code — DSL values overwrite the DB rows (mirror back).
    #
    # Returns the Hash of overrides that ended up applied.
    def reload_overrides_from_db!
      mode = DispatchPolicy.config.policy_config_source || :db

      if mode == :code
        # Mirror current DSL values into the DB so the table reflects
        # the canonical source. No-op if values already match.
        DispatchPolicy::PolicyConfig.upsert_many!(
          policy_name: @name,
          values:      config_overrides,
          source:      "code"
        )
        return config_overrides
      end

      DispatchPolicy::PolicyConfig.load_into(self)
    end

    # Persist the current in-memory overrides to the DB. Used by the
    # console / admin UI when an operator tunes a knob at runtime.
    def persist_overrides!(source: "ui")
      DispatchPolicy::PolicyConfig.upsert_many!(
        policy_name: @name,
        values:      config_overrides,
        source:      source
      )
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
