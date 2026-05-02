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
      @shard_by_proc        = nil
      @partition_by_proc    = nil
      @fairness_half_life_seconds = nil
      @tick_admission_budget = nil
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

    # Per-policy override for the EWMA half-life used to weigh recent
    # admissions when reordering claimed partitions inside the tick.
    # Accepts a Numeric (seconds) or any object responding to `to_f`
    # (so ActiveSupport durations like `30.seconds` work too).
    #   fairness half_life: 30.seconds
    def fairness(half_life: nil)
      @fairness_half_life_seconds = Float(half_life) if half_life
    end

    # Per-policy override for the global tick admission cap. nil
    # (default) means use config.tick_admission_budget; if that's also
    # nil, no global cap is enforced and per-partition admission_batch_size
    # is the only ceiling.
    def tick_admission_budget(value)
      @tick_admission_budget = Integer(value)
    end

    # Defines the partition scope. Required — every policy declares
    # exactly one. Every gate in the policy uses this proc to compute
    # the scope it enforces against (the staged_jobs row, the throttle
    # bucket on that row, and the concurrency gate's inflight rows all
    # share the same canonical key).
    #
    #   dispatch_policy :endpoints do
    #     partition_by ->(ctx) { ctx[:endpoint_id] }
    #     gate :throttle,    rate: 60, per: 60
    #     gate :concurrency, max: 5
    #   end
    #
    # If you need different scopes per gate (e.g. throttle by endpoint
    # AND concurrency by account), use two policies and let one chain
    # into the other.
    def partition_by(callable = nil, &block)
      @partition_by_proc = callable || block
    end

    # Routes a partition to a specific shard. The proc receives the
    # enriched Context (which includes :queue_name from the job) and
    # returns a string. Tick loops can be scoped per-shard so multiple
    # workers can process a single policy in parallel.
    #
    #   shard_by ->(ctx) { ctx[:queue_name] }                   # shard = job's queue
    #   shard_by ->(ctx) { "shard-#{ctx[:account_id].hash % 4}" } # explicit hash
    #
    # IMPORTANT: shard_by must be CONSISTENT with the gate's
    # `partition_by` of any rate/concurrency budget you want to enforce
    # globally. A throttle gate's bucket lives on the partition row, so
    # if two staged_partitions sharing the same throttle key end up on
    # different shards, each shard runs its own bucket and the effective
    # rate becomes rate × N_shards.
    def shard_by(callable = nil, &block)
      @shard_by_proc = callable || block
    end

    def to_policy
      Policy.new(
        name:                 @name,
        context_proc:         @context_proc,
        gates:                @gates,
        retry_strategy:       @retry_strategy,
        queue_name:           @queue_name,
        admission_batch_size: @admission_batch_size,
        shard_by_proc:        @shard_by_proc,
        partition_by_proc:    @partition_by_proc,
        fairness_half_life_seconds: @fairness_half_life_seconds,
        tick_admission_budget: @tick_admission_budget
      )
    end
  end
end
