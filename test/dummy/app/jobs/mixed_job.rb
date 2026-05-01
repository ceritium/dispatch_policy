# frozen_string_literal: true

class MixedJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :mixed do
    context ->(args) {
      attrs = args.first || {}
      {
        endpoint_id: attrs["endpoint_id"] || "ep:default",
        account_id:  attrs["account_id"]  || "acct:default",
        rate:        Integer(attrs["rate"] || 10),
        per:         Integer(attrs["per"]  || 60),
        max:         Integer(attrs["max"]  || 2)
      }
    }

    gate :throttle,
         rate:         ->(c) { c[:rate] },
         per:          60,
         partition_by: ->(c) { c[:endpoint_id] }

    gate :concurrency,
         max:          ->(c) { c[:max] },
         partition_by: ->(c) { c[:account_id] }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("MIXED_SLEEP", "0.7")))
    Rails.logger.info("[MixedJob] ep=#{attrs['endpoint_id']} acct=#{attrs['account_id']}")
  end
end
