# frozen_string_literal: true

module DispatchPolicy
  class Policy
    attr_reader :name, :context_proc, :gates, :retry_strategy, :queue_name, :admission_batch_size

    def initialize(name:, context_proc:, gates:, retry_strategy: :restage, queue_name: nil, admission_batch_size: nil)
      @name                 = name.to_s
      @context_proc         = context_proc
      @gates                = gates.freeze
      @retry_strategy       = retry_strategy
      @queue_name           = queue_name
      @admission_batch_size = admission_batch_size

      validate!
    end

    def build_context(arguments)
      result = if context_proc
                 context_proc.call(arguments)
               else
                 {}
               end
      Context.wrap(result)
    end

    def partition_key_for(ctx)
      gates.map { |gate| "#{gate.name}=#{gate.partition_for(ctx)}" }.join("|")
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
