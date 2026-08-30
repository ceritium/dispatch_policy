# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# A backoff is derived from a token debt, and a forced admission charges
# the bucket for everything it forwarded — so `retry_after` has no bound.
# Postgres' interval INPUT PARSER rejects a seconds field above INT_MAX,
# which a `rate: 2, per: 7.days` policy crosses after ~7,100 drained jobs,
# inside a single drain click.
#
# What made that catastrophic rather than annoying is where the statement
# lives: bulk_record_partition_denies! writes ONE UPDATE for the whole
# tick, and Tick#flush_denies! only logs on failure. So one unparseable
# interval discarded every denied partition's backoff and gate_state
# patch in that batch, and those partitions were re-claimed every tick
# with nothing recorded — the M4 busy-loop, for a whole policy.
class IntervalOverflowTest < DispatchPolicy::IntegrationTest
  POLICY = "interval_overflow"

  def gate
    DispatchPolicy::Gates::Throttle.new(rate: 2, per: 7 * 24 * 3600)
  end

  def seed_partition!(key, pending: 1)
    DispatchPolicy::Repository.upsert_partition!(
      policy_name: POLICY, partition_key: key, queue_name: nil,
      context: {}, delta_pending: pending
    )
  end

  def next_eligible(key)
    DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: key).next_eligible_at
  end

  # (1 - (-10_000)) / (2 / 604800.0) is about 3.02e9 seconds — past INT_MAX.
  def huge_backoff
    g = gate
    seed_partition!("debtor")
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET gate_state = jsonb_build_object('throttle', jsonb_build_object(
            'tokens', -10000.0, 'refilled_at', EXTRACT(EPOCH FROM now())))
      WHERE partition_key = 'debtor'
    SQL
    row = DispatchPolicy::Repository.normalize_partition(
      ActiveRecord::Base.connection.exec_query(
        "SELECT * FROM dispatch_policy_partitions WHERE partition_key = 'debtor'"
      ).first
    )
    decision = g.evaluate(DispatchPolicy::Context.wrap({}), row, 100)
    assert_operator decision.retry_after, :>, 2_147_483_647,
                    "the scenario needs a debt past INT_MAX to be worth testing"
    decision.retry_after
  end

  def test_a_backoff_past_int_max_is_recorded_rather_than_rejected
    retry_after = huge_backoff

    DispatchPolicy::Repository.bulk_record_partition_denies!([{
      policy_name: POLICY, partition_key: "debtor",
      gate_state_patch: {}, retry_after: retry_after
    }])

    refute_nil next_eligible("debtor"),
               "an unparseable interval left the partition with no backoff at all"
  end

  # The blast radius is what makes this a must-fix: the flush is one
  # statement for the whole tick and Tick#flush_denies! swallows failures.
  def test_one_oversized_backoff_does_not_discard_the_whole_batch
    retry_after = huge_backoff
    seed_partition!("bystander")

    DispatchPolicy::Repository.bulk_record_partition_denies!([
      { policy_name: POLICY, partition_key: "debtor",
        gate_state_patch: {}, retry_after: retry_after },
      { policy_name: POLICY, partition_key: "bystander",
        gate_state_patch: { "throttle" => { "tokens" => 1.0 } }, retry_after: 30 }
    ])

    refute_nil next_eligible("bystander"),
               "every partition denied in the same tick lost its backoff with it"
  end

  # The single-partition path builds the same expression.
  def test_the_admit_path_records_an_oversized_backoff_too
    retry_after = huge_backoff

    DispatchPolicy::Repository.record_partition_admit!(
      policy_name: POLICY, partition_key: "debtor", admitted: 0,
      gate_state_patch: {}, retry_after: retry_after
    )

    refute_nil next_eligible("debtor")
  end
end
