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

    # partition_by combines endpoint and account so both gates enforce
    # against the same canonical scope. If you wanted "throttle per
    # endpoint AND concurrency per account" with different scopes,
    # split into two policies and chain them.
    partition_by ->(c) { "ep:#{c[:endpoint_id]}|acct:#{c[:account_id]}" }

    gate :throttle,    rate: ->(c) { c[:rate] }, per: 60
    gate :concurrency, max:  ->(c) { c[:max] }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("MIXED_SLEEP", "0.7")))
    Rails.logger.info("[MixedJob] ep=#{attrs['endpoint_id']} acct=#{attrs['account_id']}")
  end
end
