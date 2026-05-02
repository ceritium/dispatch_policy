# frozen_string_literal: true

# For `duration_seconds` seconds, enqueue jobs for many distinct
# accounts/tenants with a triangular weight distribution: account
# acc_N is N× more likely to be picked than acc_1. Lets the operator
# exercise the admin UI, the fairness reorder, and the
# adaptive_concurrency cap with hundreds of partitions and a skewed
# distribution.
#
# Picks the target job class by name so the same storm form can drive
# AdaptiveDemoJob, FairnessDemoJob, or HighThrottleJob. Defaults to
# adaptive_demo because that's where the AIMD is interesting under
# load.
class StormJob < ApplicationJob
  queue_as :default

  TARGETS = {
    "adaptive_demo"     => AdaptiveDemoJob,
    "fairness_demo"     => FairnessDemoJob,
    "high_throttle"     => HighThrottleJob,
    "high_concurrency"  => HighConcurrencyJob
  }.freeze

  def perform(num_accounts: 100, duration_seconds: 60, batch_size: 20, tick_ms: 200, target: "adaptive_demo")
    klass = TARGETS.fetch(target) { TARGETS.fetch("adaptive_demo") }
    deadline = Time.current + duration_seconds
    total_weight = num_accounts * (num_accounts + 1) / 2

    Rails.logger.info("[StormJob] starting target=#{klass} accounts=#{num_accounts} duration=#{duration_seconds}s")

    while Time.current < deadline
      jobs = Array.new(batch_size) {
        idx = weighted_sample(num_accounts, total_weight)
        klass.new(attrs_for(klass, idx))
      }
      ActiveJob.perform_all_later(jobs)
      sleep(tick_ms / 1000.0)
    end

    Rails.logger.info("[StormJob] done target=#{klass}")
  end

  private

  # The tenant key field name varies per job; pass through whatever the
  # target's context proc reads.
  def attrs_for(klass, idx)
    case klass.name
    when "AdaptiveDemoJob", "FairnessDemoJob"
      { "tenant" => "acc_#{idx}" }
    when "HighThrottleJob"
      { "endpoint" => "acc_#{idx}", "rate" => 10_000 }
    when "HighConcurrencyJob"
      { "bucket" => "acc_#{idx}", "max" => 100 }
    else
      { "tenant" => "acc_#{idx}" }
    end
  end

  # Pick i in 1..n with P(i) ∝ i. Linear scan; fine for the few-hundred
  # account scale this is aimed at.
  def weighted_sample(n, total_weight)
    r   = rand(total_weight)
    sum = 0
    (1..n).each do |id|
      sum += id
      return id if r < sum
    end
    n
  end
end
