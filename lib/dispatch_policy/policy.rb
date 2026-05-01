# frozen_string_literal: true

module DispatchPolicy
  # Policy = the configured admission rules for one ActiveJob class.
  # Built from the `dispatch_policy do ... end` DSL inside a job.
  #
  # Concurrency and throttle aren't filter objects in this version;
  # they're properties projected straight into dispatch_policy_partitions
  # at stage time. Concurrency cap and throttle rate must be literals
  # (Integer / Numeric); for runtime tuning use the DB-backed
  # PolicyConfig overrides.
  class Policy
    attr_reader :job_class

    def initialize(job_class, &block)
      @job_class            = job_class
      @name                 = job_class.name.underscore.tr("/", "-")
      @context_builder      = ->(_args) { {} }
      @dedupe_key_builder   = nil
      @partition_builder    = nil

      @concurrency_max      = nil
      @throttle_rate        = nil   # tokens / second
      @throttle_burst       = nil

      # Per-policy config overrides (effective_*). Same as before.
      @override_batch_size                          = nil
      @override_lease_duration                      = nil
      @override_unblock_sweep                       = nil
      @override_auto_tune                           = nil

      instance_eval(&block) if block

      validate!

      DispatchPolicy.registry[@name] = job_class
    end

    # ─── DSL ─────────────────────────────────────────────────────

    def name(value = nil)
      return @name if value.nil?
      DispatchPolicy.registry.delete(@name)
      @name = value.to_s
      DispatchPolicy.registry[@name] = @job_class
    end

    def context(builder)
      @context_builder = builder
    end

    attr_reader :context_builder

    def dedupe_key(builder)
      @dedupe_key_builder = builder
    end

    # Required when concurrency or throttle is declared. Receives the
    # job's arguments and returns a String partition key.
    def partition_by(builder)
      @partition_builder = builder
    end

    # gate :concurrency, max: N → concurrency max: N
    def concurrency(max:)
      raise ArgumentError, "concurrency max must be a positive Integer (got #{max.inspect})" \
        unless max.is_a?(Integer) && max.positive?
      @concurrency_max = max
    end

    # throttle rate: 60, per: 60.seconds, burst: 60
    def throttle(rate:, per: 1.0, burst: nil)
      per_seconds = per.is_a?(ActiveSupport::Duration) ? per.to_f : Float(per)
      raise ArgumentError, "throttle rate must be > 0" unless rate.is_a?(Numeric) && rate.positive?
      raise ArgumentError, "throttle per must be > 0"  unless per_seconds.positive?

      @throttle_rate  = (rate.to_f / per_seconds).round(6)
      @throttle_burst = (burst || rate).to_i
    end

    # ─── Builders used at stage / dispatch time ──────────────────

    def build_dedupe_key(arguments)
      return nil unless @dedupe_key_builder
      @dedupe_key_builder.call(arguments)&.to_s
    end

    def build_partition_key(arguments)
      return "default" unless @partition_builder
      key = @partition_builder.call(arguments)
      key.nil? || key.to_s.empty? ? "default" : key.to_s
    end

    attr_reader :concurrency_max, :throttle_rate, :throttle_burst

    def partitioned?
      !@partition_builder.nil?
    end

    # ─── Per-policy config overrides ─────────────────────────────

    def batch_size(value = NIL_DEFAULT)
      return effective_batch_size if value.equal?(NIL_DEFAULT)
      @override_batch_size = value
    end

    def lease_duration(value = NIL_DEFAULT)
      return effective_lease_duration if value.equal?(NIL_DEFAULT)
      @override_lease_duration = value
    end

    def auto_tune(value = NIL_DEFAULT)
      return effective_auto_tune if value.equal?(NIL_DEFAULT)
      unless [ false, nil, :recommend, :apply ].include?(value)
        raise ArgumentError, "auto_tune must be false, :recommend or :apply (got #{value.inspect})"
      end
      @override_auto_tune = value
    end

    def effective_batch_size
      @override_batch_size || DispatchPolicy.config.batch_size
    end

    def effective_lease_duration
      @override_lease_duration || DispatchPolicy.config.lease_duration
    end

    def effective_auto_tune
      return @override_auto_tune unless @override_auto_tune.nil?
      DispatchPolicy.config.auto_tune
    end

    def config_overrides
      {
        batch_size:     @override_batch_size,
        lease_duration: @override_lease_duration
      }.compact
    end

    NIL_DEFAULT = Object.new.freeze
    private_constant :NIL_DEFAULT

    # ─── Live config ─────────────────────────────────────────────

    def reload_overrides_from_db!
      mode = DispatchPolicy.config.policy_config_source || :db
      if mode == :code
        DispatchPolicy::PolicyConfig.upsert_many!(
          policy_name: @name, values: config_overrides, source: "code"
        )
        return config_overrides
      end
      DispatchPolicy::PolicyConfig.load_into(self)
    end

    def persist_overrides!(source: "ui")
      DispatchPolicy::PolicyConfig.upsert_many!(
        policy_name: @name, values: config_overrides, source: source
      )
    end

    # ─── Validation ──────────────────────────────────────────────

    def validate!
      gate_declared = !@concurrency_max.nil? || !@throttle_rate.nil?
      if gate_declared && @partition_builder.nil?
        raise ArgumentError,
          "policy #{@name}: declares concurrency or throttle but no partition_by. " \
          "Either declare `partition_by ->(args) { ... }` or remove the gates."
      end
    end
    private :validate!
  end
end
