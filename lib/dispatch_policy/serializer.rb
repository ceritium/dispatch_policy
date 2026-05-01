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

      klass = job_class.constantize
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
