# frozen_string_literal: true

# Purpose-built demo to show in-tick fairness in action.
#
# Three things make the contrast visible:
#
#   1. partition_by: ->(ctx) { ctx[:tenant] }
#      Several partitions in the same policy.
#
#   2. tick_admission_budget 30
#      Enforces fair_share = ceil(30 / N_partitions) per tick. With 5
#      claimed partitions the cap is 6 admits per partition per tick;
#      pass-2 redistributes leftover.
#
#   3. fairness half_life: 30.seconds
#      Half-life short enough that the EWMA reorder reacts within the
#      lifetime of the demo (default 60s would also work, just slower).
#
# Drive it from the home page form: "Flood the unfair workload". One
# tenant gets 500 jobs, four others get 10 each. Without fairness, the
# 500-job tenant would be served first on every tick and the 10-job
# tenants would queue behind. With fairness (decayed_admits ASC + fair
# share), the cold tenants drain in the first 2 ticks while the hot
# one progresses at its capped rate.
#
# Watch /dispatch_policy/policies/fairness_demo while it runs. The
# decayed_admits column on the partition page is the EWMA value being
# used to order partitions inside each tick.
class FairnessDemoJob < ApplicationJob
  queue_as :default

  dispatch_policy_inflight_tracking

  dispatch_policy :fairness_demo do
    context ->(args) {
      attrs = args.first || {}
      { tenant: attrs["tenant"] || attrs[:tenant] || "unknown" }
    }

    # Single canonical partition scope at the policy level. Both the
    # throttle bucket (lives in partitions.gate_state) and any
    # concurrency cap (would read inflight_jobs by the same key) share
    # this scope — no rate dilution.
    partition_by ->(c) { "tenant:#{c[:tenant]}" }

    # rate high enough that the throttle never binds — the only thing
    # gating admission is fair_share + tick_admission_budget.
    gate :throttle, rate: 10_000, per: 60

    fairness half_life: 30.seconds
    # tick_admission_budget 30
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("FAIRNESS_DEMO_SLEEP", "0.05")))
    Rails.logger.info(
      "[FairnessDemoJob] tenant=#{attrs['tenant']} seq=#{attrs['seq']} ran at #{Time.current.iso8601}"
    )
  end
end
