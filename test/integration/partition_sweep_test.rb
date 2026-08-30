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

  class HalfWeeklyJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("sweep_half_weekly") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 0.5, per: 7 * 24 * 3600
    end

    def perform(*); end
  end

  class UngatedJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("sweep_ungated") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(WeeklyJob::POLICY, owner: WeeklyJob.name)
    DispatchPolicy.registry.register(MinuteJob::POLICY, owner: MinuteJob.name)
    DispatchPolicy.registry.register(HalfWeeklyJob::POLICY, owner: HalfWeeklyJob.name)
    DispatchPolicy.registry.register(UngatedJob::POLICY, owner: UngatedJob.name)
  end

  # Ages the WHOLE row, token bucket included. Moving last_admit_at back
  # without moving the bucket's refilled_at describes a state the clock
  # cannot produce — an hour of idleness during which no refill happened
  # — and the sweeper's question is precisely "has this bucket refilled
  # yet?", so a helper that lies about it can only test the wrong thing.
  def age_partitions!(hours)
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET last_admit_at = now() - interval '#{hours} hours',
          created_at    = now() - interval '#{hours} hours',
          gate_state    = CASE
            WHEN gate_state ? 'throttle'
            THEN jsonb_set(gate_state, '{throttle,refilled_at}',
                   to_jsonb((gate_state -> 'throttle' ->> 'refilled_at')::double precision
                            - #{hours} * 3600))
            ELSE gate_state
          END
    SQL
  end

  def bucket
    DispatchPolicy::Partition.first.gate_state.dig("throttle", "tokens").to_f
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
  # collected, or it accumulates forever. With nothing in its gate_state
  # there is nothing to lose by collecting it on the usual cutoff.
  def test_partitions_of_an_unregistered_policy_with_no_bucket_are_collected
    UngatedJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_ungated")
    age_partitions!(25)

    DispatchPolicy.registry.clear
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "the catch-all pass covers policies this process doesn't know"
  end

  # ...but "absent from this process's registry" is not "deleted from the
  # code". The registry fills as a side effect of job classes loading, so
  # a dashboard process, a lazily-loaded worker or a half-rolled deploy
  # all get here with a policy the rest of the fleet knows perfectly well
  # — and collecting the row resets its token bucket, which is the M11
  # quota reset with extra steps. Same trap as ISSUES.md R3.
  def test_an_unregistered_policys_token_bucket_is_not_reset_on_the_normal_cutoff
    2.times { WeeklyJob.perform_later }
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")
    assert_equal 0, DispatchPolicy::StagedJob.count, "the weekly quota is spent"

    age_partitions!(25)
    DispatchPolicy.registry.clear
    DispatchPolicy::TickLoop.sweep!

    assert_equal 1, DispatchPolicy::Partition.count,
                 "this process cannot know the window, so it must not guess 24h"

    age_partitions!(31 * 24) # past unknown_policy_retention
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "a genuinely deleted policy still gets collected, just later"
  end
  # Keeping a partition for the whole window is only necessary while its
  # bucket is BELOW capacity — that is the state worth preserving. Once it
  # has refilled the row holds nothing (a partition that reappears starts
  # full), so it can be collected on the normal cutoff instead of being
  # held for a week.
  #
  # Spending ONE of the two weekly tokens is what makes this test bite:
  # the bucket climbs back to capacity in 3.5 days, well inside the 7-day
  # window the old rule would have waited out. The state is produced by a
  # real admission plus real idleness — writing `tokens` by hand would
  # test a row the running system never creates, since the admission
  # UPDATE is the only writer of that key and it always subtracts.
  def test_a_refilled_bucket_is_collected_before_its_window_is_out
    WeeklyJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")
    assert_in_delta 1.0, bucket, 0.01, "one of two weekly tokens spent"

    age_partitions!(4 * 24) # refills in 3.5 days; still inside the 7-day window
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "a full bucket is worth nothing; holding the row for 7 days is pure bloat"
  end

  def test_a_partly_spent_bucket_is_still_held
    WeeklyJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")

    age_partitions!(3 * 24) # half a token short of the 3.5 days it needs
    DispatchPolicy::TickLoop.sweep!

    assert_equal 1, DispatchPolicy::Partition.count,
                 "still short of capacity means quota this tenant has spent"
  end

  # The bucket goes NEGATIVE when two tick loops cover one (policy, shard)
  # and both admit against the same snapshot — that overdraft is how the
  # long-run rate survives the burst. Collecting the row before it is
  # repaid forgives the debt, which is the M11 reset wearing a new hat:
  # one window of refill only takes -2 back to 0, not to capacity.
  def test_a_bucket_in_debt_is_not_collected_when_the_window_is_out
    2.times { WeeklyJob.perform_later }
    DispatchPolicy::Tick.run(policy_name: "sweep_weekly")

    # A second loop admitting against the same pre-admission snapshot.
    gate     = WeeklyJob::POLICY.gates.find { |g| g.name == :throttle }
    snapshot = { "gate_state" => {} }
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), snapshot, 100)
    DispatchPolicy::Repository.record_partition_admit!(
      policy_name: "sweep_weekly", partition_key: "k", admitted: 2,
      gate_state_patch: {}, retry_after: nil, throttle_charge: decision.charge
    )
    assert_in_delta(-2.0, bucket, 0.01, "four admits against a bucket of two")

    age_partitions!(8 * 24) # a whole window past — but the debt needs two
    DispatchPolicy::TickLoop.sweep!

    assert_equal 1, DispatchPolicy::Partition.count,
                 "deleting it here hands the tenant a full bucket and erases the overdraft"
  end

  # A sub-unit rate refills one token in `per / rate` seconds, not in
  # `per`: capacity is floored at 1.0 so the bucket can ever admit, while
  # the refill still runs at the true rate. Collecting on the window would
  # hand out a token the tenant has not earned.
  def test_a_sub_unit_rate_is_held_past_its_window
    HalfWeeklyJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "sweep_half_weekly")
    assert_in_delta 0.0, bucket, 0.01

    age_partitions!(8 * 24) # past the 7-day window, half of the 14 days owed
    DispatchPolicy::TickLoop.sweep!

    assert_equal 1, DispatchPolicy::Partition.count,
                 "one job per 14 days: a window of refill is only half a token"

    age_partitions!(15 * 24)
    DispatchPolicy::TickLoop.sweep!

    assert_equal 0, DispatchPolicy::Partition.count,
                 "once the token is genuinely back, the row is garbage"
  end
end
