# frozen_string_literal: true

# Stress test: very high throttle rate, almost unconstrained.
#
# With rate=10000/min the bucket fills almost instantly; the limiter is
# effectively the worker pool. Useful for measuring tick duration, admit
# batch saturation, and forwarder throughput under load.
class HighThrottleJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :high_throttle do
    context ->(args) {
      attrs = args.first || {}
      {
        endpoint: attrs["endpoint"] || attrs[:endpoint] || "default",
        rate:     Integer(attrs["rate"] || attrs[:rate] || 10_000)
      }
    }

    gate :throttle,
         rate:         ->(c) { c[:rate] },
         per:          60,
         partition_by: ->(c) { "ep:#{c[:endpoint]}" }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("HIGH_THROTTLE_SLEEP", "0.05")))
    Rails.logger.info("[HighThrottleJob] endpoint=#{attrs['endpoint']} seq=#{attrs['seq']} ran at #{Time.current.iso8601}")
  end
end
