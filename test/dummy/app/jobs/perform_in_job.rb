# frozen_string_literal: true

class PerformInJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :perform_in_demo do
    context ->(args) { { key: (args.first || {})["key"] || "default" } }
    partition_by ->(c) { c[:key] }
    gate :throttle, rate: 30, per: 60
  end

  def perform(attrs = {})
    Rails.logger.info("[PerformInJob] key=#{attrs['key']} ran at #{Time.current.iso8601}")
  end
end
