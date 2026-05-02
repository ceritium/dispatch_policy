# frozen_string_literal: true

# Stress test: very high concurrency cap, very short jobs.
#
# With max=100 and a 100ms perform, expect to see ~100 active rows in
# dispatch_policy_inflight_jobs at once when you flood the queue. Useful for
# watching the inflight counter, total_admitted, and tick duration grow.
class HighConcurrencyJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :high_concurrency do
    context ->(args) {
      attrs = args.first || {}
      {
        bucket: attrs["bucket"] || attrs[:bucket] || "default",
        max:    Integer(attrs["max"] || attrs[:max] || 100)
      }
    }

    partition_by ->(c) { "bucket:#{c[:bucket]}" }
    gate :concurrency, max: ->(c) { c[:max] }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("HIGH_CONCURRENCY_SLEEP", "0.1")))
    Rails.logger.info("[HighConcurrencyJob] bucket=#{attrs['bucket']} seq=#{attrs['seq']} ran at #{Time.current.iso8601}")
  end
end
