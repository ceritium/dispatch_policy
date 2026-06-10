# frozen_string_literal: true

module DispatchPolicy
  class Context
    def self.wrap(value)
      case value
      when Context then value
      when Hash    then new(value)
      when nil     then new({})
      else
        raise InvalidPolicy, "context must be a Hash, got #{value.class}"
      end
    end

    attr_reader :data

    def initialize(hash)
      @data = deep_stringify(hash).freeze
    end

    def [](key)
      indifferent(@data[key.to_s])
    end

    def to_h
      @data
    end

    def to_jsonb
      @data
    end

    def fetch(key, *args, &block)
      indifferent(@data.fetch(key.to_s, *args, &block))
    end

    private

    # Nested hashes are stored string-keyed (deep_stringify), so
    # `ctx[:limits][:max]` would miss — the inner lookup is by symbol.
    # Return nested hashes with indifferent access so symbol and string
    # keys work at every depth, matching how host apps usually write
    # context. to_h/to_jsonb still return the plain string-keyed hash for
    # storage, untouched.
    def indifferent(value)
      value.is_a?(Hash) ? value.with_indifferent_access : value
    end

    def deep_stringify(value)
      case value
      when Hash
        value.each_with_object({}) { |(k, v), m| m[k.to_s] = deep_stringify(v) }
      when Array
        value.map { |v| deep_stringify(v) }
      else
        value
      end
    end
  end
end
