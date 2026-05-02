# frozen_string_literal: true

# Demo for :adaptive_concurrency. The cap per partition self-tunes:
# grows on quiet periods, shrinks when the adapter queue builds up.
#
# How to see it in the UI:
#
#   1. Flood with the home form: pick a tenant and a count.
#   2. Watch /dispatch_policy/policies/adaptive_demo. Each partition's
#      `current_max` lives in dispatch_policy_adaptive_concurrency_stats.
#   3. The perform sleeps for `ADAPTIVE_DEMO_SLEEP` seconds (default
#      0.3). That makes the queue back up under bursts and shrink the
#      cap; a quiet period of 30s+ lets it grow back.
#
# `target_lag_ms: 500` is intentionally tight so the AIMD reactions
# are easy to observe in seconds, not minutes.
class AdaptiveDemoJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :adaptive_demo do
    context ->(args) {
      attrs = args.first || {}
      { tenant: attrs["tenant"] || attrs[:tenant] || "default" }
    }
    partition_by ->(c) { "tenant:#{c[:tenant]}" }

    # Self-tuning concurrency cap per tenant (current_max).
    gate :adaptive_concurrency,
         initial_max:   3,
         target_lag_ms: 500,
         min:           1

    # In-tick fairness: half-life short so the demo reacts visibly
    # within seconds. fair_share = ceil(tick_admission_budget / N)
    # caps each claimed partition per tick — composes with the
    # adaptive cap as `min(fair_share, current_max - in_flight)`.
    fairness half_life: 20.seconds
    tick_admission_budget 30
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("ADAPTIVE_DEMO_SLEEP", "0.3")))
    Rails.logger.info("[AdaptiveDemoJob] tenant=#{attrs['tenant']} ran at #{Time.current.iso8601}")
  end
end
