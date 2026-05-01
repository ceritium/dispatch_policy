# frozen_string_literal: true

module DispatchPolicy
  class Policy
    DEFAULT_SHARD = "default"

    attr_reader :name, :context_proc, :gates, :retry_strategy, :queue_name,
                :admission_batch_size, :shard_by_proc

    def initialize(name:, context_proc:, gates:, retry_strategy: :restage,
                   queue_name: nil, admission_batch_size: nil, shard_by_proc: nil)
      @name                 = name.to_s
      @context_proc         = context_proc
      @gates                = gates.freeze
      @retry_strategy       = retry_strategy
      @queue_name           = queue_name
      @admission_batch_size = admission_batch_size
      @shard_by_proc        = shard_by_proc

      validate!
    end

    # Builds the Context the gates and shard_by will see at admission time.
    # The user's context_proc receives the job's arguments. The gem then
    # enriches the resulting hash with `queue_name` (the ActiveJob queue)
    # so shard_by/partition_by can route by queue without the user having
    # to thread it through their proc.
    def build_context(arguments, queue_name: nil)
      base = context_proc ? context_proc.call(arguments) : {}
      base = (base || {}).to_h
      base = base.merge(queue_name: queue_name) if queue_name
      Context.wrap(base)
    end

    def partition_key_for(ctx)
      gates.map { |gate| "#{gate.name}=#{gate.partition_for(ctx)}" }.join("|")
    end

    # The shard a partition belongs to. Stable per (policy, partition_key)
    # via first-writer-wins in Repository.upsert_partition!. If no shard_by
    # is declared the partition lives on the "default" shard.
    def shard_for(ctx)
      return DEFAULT_SHARD unless @shard_by_proc

      value = @shard_by_proc.call(ctx)
      value.nil? ? DEFAULT_SHARD : value.to_s
    end

    def restage_retries?
      retry_strategy == :restage
    end

    def bypass_retries?
      retry_strategy == :bypass
    end

    private

    def validate!
      raise InvalidPolicy, "policy name required" if @name.empty?
      raise InvalidPolicy, "at least one gate required" if @gates.empty?
      unless %i[restage bypass].include?(@retry_strategy)
        raise InvalidPolicy, "retry_strategy must be :restage or :bypass"
      end
    end
  end
end
