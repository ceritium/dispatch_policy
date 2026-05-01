# frozen_string_literal: true

class SlowExternalApiJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :slow_api do
    context ->(args) {
      attrs = args.first || {}
      {
        endpoint_id: attrs["endpoint_id"] || attrs[:endpoint_id] || "default",
        rate:        Integer(attrs["rate"]    || attrs[:rate]    || 6),
        per:         Integer(attrs["per"]     || attrs[:per]     || 60)
      }
    }

    gate :throttle,
         rate:         ->(c) { c[:rate] },
         per:          60,
         partition_by: ->(c) { c[:endpoint_id] }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("SLOW_API_SLEEP", "0.5")))
    Rails.logger.info("[SlowExternalApiJob] endpoint=#{attrs['endpoint_id']} run at #{Time.current.iso8601}")
  end
end
