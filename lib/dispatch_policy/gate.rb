# frozen_string_literal: true

module DispatchPolicy
  class Gate
    class << self
      def registry
        @registry ||= {}
      end

      def register(name, klass)
        registry[name.to_sym] = klass
      end
    end

    attr_reader :policy, :partition_by, :name

    def initialize(policy:, name:, partition_by: nil, **opts)
      @policy       = policy
      @name         = name
      @partition_by = partition_by
      configure(**opts)
    end

    def configure(**_opts); end

    # Resolve a partition key for a given context.
    def partition_key_for(ctx)
      return "default" if @partition_by.nil?
      @partition_by.call(ctx).to_s
    end

    # Subclasses must implement.
    def filter(_batch, _context)
      raise NotImplementedError
    end

    # Whether this gate keeps an in-flight count that must be released
    # when the job finishes.
    def tracks_inflight?
      false
    end

    protected

    def resolve(value, ctx)
      value.respond_to?(:call) ? value.call(ctx) : value
    end
  end
end
