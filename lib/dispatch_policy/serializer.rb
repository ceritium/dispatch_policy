# frozen_string_literal: true

require "json"

module DispatchPolicy
  module Serializer
    module_function

    # Serialize an ActiveJob instance for storage in staged_jobs.job_data.
    # Returns a Ruby hash compatible with PostgreSQL jsonb (string keys).
    def serialize(job)
      job.serialize
    end

    # Deserialize stored job_data into a fresh ActiveJob instance ready
    # to be enqueued via `#enqueue`.
    def deserialize(payload)
      job_class = payload["job_class"] || payload[:job_class]
      raise InvalidPolicy, "missing job_class in stored payload" unless job_class

      # Split so the caller can tell "this process cannot resolve the class"
      # from anything that goes wrong afterwards. NoMethodError is a
      # NameError, so a custom argument serializer touching a nil would
      # otherwise be indistinguishable from a missing constant — and one
      # of those is worth holding a row back for, the other is not.
      klass = begin
        job_class.constantize
      rescue NameError => e
        raise UnresolvableJobClass, "#{job_class}: #{e.class}: #{e.message}"
      end
      klass.deserialize(payload)
    end

    def dump_jsonb(value)
      JSON.dump(value)
    end

    def load_jsonb(text)
      return text if text.is_a?(Hash) || text.is_a?(Array)
      return {} if text.nil? || text == ""

      JSON.parse(text)
    end
  end
end
