# frozen_string_literal: true

class BulkAccountJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :bulk_account do
    context ->(args) {
      attrs = args.first || {}
      {
        account_id:      attrs["account_id"] || attrs[:account_id] || "default",
        max_per_account: Integer(attrs["max"] || attrs[:max] || 3)
      }
    }

    partition_by ->(c) { "acct:#{c[:account_id]}" }
    gate :concurrency, max: ->(c) { c[:max_per_account] }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("BULK_SLEEP", "1.0")))
    Rails.logger.info("[BulkAccountJob] acct=#{attrs['account_id']} run at #{Time.current.iso8601}")
  end
end
