# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# M11: the token bucket lives in the partition row's gate_state, so
# collecting that row while its refill window is still running hands the
# tenant a brand new quota. With `rate: 2, per: 7.days` and the default
# 24h partition_inactive_after, two admits plus a day of quiet used to
# buy two more inside the same week — a 100% overshoot of a contractual
# rate, with nothing in the logs.
class PartitionSweepTest < DispatchPolicy::IntegrationTest
  class WeeklyJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("sweep_weekly") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 2, per: 7 * 24 * 3600
    end

    def perform(*); end
  end

  class MinuteJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("sweep_minute") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 10, per: 60
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(WeeklyJob::POLICY, owner: WeeklyJob.name)
    DispatchPolicy.registry.register(MinuteJob::POLICY, owner: MinuteJob.name)
  end

  def age_partitions!(hours)
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET last_admit_at = now() - interval '#{hours} hours',
          created_at    = now() - interval '#{hours} hours'
    SQL
  end

  def test_a_partition_is_not_collected_inside_its_refill_window
    2.times { WeeklyJob.perform_later }
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")
    assert_equal 0, DispatchPolicy::StagedJob.count, "the weekly quota is spent"

    age_partitions!(25) # past partition_inactive_after, inside the 7-day window
    DispatchPolicy::TickLoop.sweep!

    assert_equal 1, DispatchPolicy::Partition.count,
                 "collecting it here resets the bucket and doubles the weekly rate"

    2.times { WeeklyJob.perform_later }
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")
    assert_equal 2, DispatchPolicy::StagedJob.count,
                 "the bucket survived, so these stay staged until the window rolls over"
  end

  def test_a_partition_past_its_refill_window_is_still_collected
    2.times { WeeklyJob.perform_later }
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")

    age_partitions!(8 * 24) # past the 7-day window
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "once the bucket would have refilled anyway, the row is just garbage"
  end

  # A short window must not gain a longer TTL: the whole point of the
  # sweeper is keeping the partitions table small.
  def test_a_short_window_still_uses_the_default_cutoff
    MinuteJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_minute")

    age_partitions!(25)
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "per=60s is far inside partition_inactive_after; collect as before"
  end

  # A partition whose policy no longer exists in the code still has to be
  # collected, or it accumulates forever.
  def test_partitions_of_an_unregistered_policy_are_collected
    MinuteJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_minute")
    age_partitions!(25)

    DispatchPolicy.registry.clear
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "the catch-all pass covers policies this process doesn't know"
  end
end
