# frozen_string_literal: true

class RetryFlakyJob < ApplicationJob
  queue_as :default

  retry_on StandardError, wait: 1.second, attempts: 4

  dispatch_policy_inflight_tracking

  dispatch_policy :flaky do
    context ->(args) { { key: (args.first || {})["key"] || "default" } }
    gate :throttle, rate: 5, per: 60, partition_by: ->(c) { c[:key] }
    retry_strategy :restage
  end

  def perform(attrs = {})
    fail_rate = Float(attrs["fail_rate"] || 0.7)
    raise "boom" if rand < fail_rate
    Rails.logger.info("[RetryFlakyJob] key=#{attrs['key']} success on exec=#{executions}")
  end
end
