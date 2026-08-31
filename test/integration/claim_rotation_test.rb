# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/staged_job"

# In-tick fairness is ORDERING plus a CAP, and the two live in different
# places on purpose. `claim_partitions` orders by `last_checked_at NULLS
# FIRST, id` — anti-stagnation, so every partition holding pending work is
# claimed within ⌈N / batch_size⌉ ticks. The Tick then reorders what it
# claimed, in memory, by `decayed_admits` and caps each one at its fair
# share.
#
# Folding the fairness signal into the claim's ORDER BY is the obvious
# simplification and CLAUDE.md forbids it by name. Nothing enforced that:
# the edit it warns about left the whole suite green, which is precisely
# the state in which someone makes it.
#
# It starves partitions permanently, not slowly. `decayed_admits` only
# grows when a partition ADMITS, and the claim does not admit — so once a
# partition has admitted anything it sorts behind every partition that has
# not, forever, and above `partition_batch_size` candidates it is never
# claimed again. The partitions punished are exactly the ones doing work.
class ClaimRotationTest < DispatchPolicy::IntegrationTest
  POLICY     = "claim_rotation"
  BATCH      = 2
  PARTITIONS = 5

  def setup
    super
    PARTITIONS.times do |i|
      DispatchPolicy::Repository.upsert_partition!(
        policy_name: POLICY, partition_key: "p#{i}", queue_name: nil,
        context: {}, delta_pending: 1
      )
    end
  end

  def claim_round
    DispatchPolicy::Repository
      .claim_partitions(policy_name: POLICY, limit: BATCH)
      .map { |p| p["partition_key"] }
  end

  def test_a_partition_that_has_admitted_is_still_claimed
    # p0 is the one that has been doing the work. Under the shipped order
    # that buys it nothing; under a decayed_admits order it is a sentence.
    DispatchPolicy::Partition
      .find_by(policy_name: POLICY, partition_key: "p0")
      .update!(decayed_admits: 500.0, decayed_admits_at: Time.now)

    seen = []
    # Three rounds is ⌈5 / 2⌉ = 3, the guarantee itself: every partition
    # holding pending work gets claimed inside that many ticks.
    3.times { seen.concat(claim_round) }

    assert_includes seen, "p0",
                    "a partition that has admitted must still be claimed: ordering the " \
                    "claim by decayed_admits sorts it behind every partition that has " \
                    "not admitted, and it never comes back"
    assert_equal PARTITIONS, seen.uniq.size,
                 "every partition with pending work is claimed within ceil(N/batch) ticks"
  end

  # The other half of the same invariant: the rotation has to be driven by
  # `last_checked_at`, so a partition just claimed goes to the BACK.
  def test_the_claim_rotates_instead_of_returning_the_same_head
    first  = claim_round
    second = claim_round

    assert_equal BATCH, first.size
    assert_empty first & second,
                 "last_checked_at is bumped on claim; returning the same partitions " \
                 "twice in a row starves everything behind them"
  end
end
