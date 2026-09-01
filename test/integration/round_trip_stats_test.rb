# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"

# A8: the round-trip figures answer "is the tick getting round its
# partitions?", and a partition parked on future work is not one the tick
# is supposed to reach. Counting it made an ordinary `set(wait:)` workload
# turn the "N active partitions have never been checked — increase
# partition_batch_size or shard" hint on permanently, pointing the operator
# at the one knob that cannot help, and dragged the p95 round trip toward
# infinity while the tick was perfectly healthy.
class RoundTripStatsTest < DispatchPolicy::IntegrationTest
  POLICY = "round_trip"

  def make_partition(key, horizon: nil, checked_ago: nil)
    DispatchPolicy::Repository.upsert_partition!(
      policy_name: POLICY, partition_key: key, queue_name: nil,
      context: {}, delta_pending: 1, scheduled_at: horizon
    )
    return if checked_ago.nil?

    DispatchPolicy::Partition.where(policy_name: POLICY, partition_key: key)
                             .update_all(last_checked_at: Time.now.utc - checked_ago)
  end

  def stats
    DispatchPolicy::Repository.partition_round_trip_stats(policy_name: POLICY)
  end

  def test_a_schedule_parked_partition_is_not_counted_as_never_checked
    make_partition("parked", horizon: Time.now.utc + 3600)
    make_partition("waiting")

    assert_equal 2, stats[:active_partitions], "both hold pending work"
    assert_equal 1, stats[:never_checked],
                 "only the partition the tick could have claimed and did not"
    assert_equal 1, stats[:schedule_parked]
  end

  # The other end of the same mistake: a parked partition's last_checked_at
  # stands still by design, so leaving it in the percentiles reports a
  # round trip the tick is not actually taking.
  def test_a_schedule_parked_partition_does_not_inflate_the_age_percentiles
    make_partition("parked", horizon: Time.now.utc + 3600, checked_ago: 86_400)
    make_partition("fresh", checked_ago: 10)

    assert_in_delta 10, stats[:oldest_age_seconds], 5,
                    "the oldest CLAIMABLE partition was checked 10s ago"
    assert_in_delta 10, stats[:p95_age_seconds], 5
  end

  # A horizon that has arrived is claimable again and counts normally —
  # the exclusion is "waiting on the clock", not "has ever been scheduled".
  def test_a_partition_whose_horizon_has_passed_counts_again
    make_partition("due", horizon: Time.now.utc - 60)

    assert_equal 0, stats[:schedule_parked]
    assert_equal 1, stats[:never_checked]
  end
end
