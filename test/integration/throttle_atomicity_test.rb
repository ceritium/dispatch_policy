# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"

# The token bucket is settled inside the admission UPDATE, from the row's
# own value, instead of being written back from a number Ruby computed
# earlier. That read-modify-write was a real hole: two tick loops covering
# the same (policy, shard) each evaluated a full bucket, each admitted it,
# and the second write overwrote the first — so one of the two admissions
# was never charged and the effective rate became rate x loops, forever.
#
# One tick loop per (policy, shard) is still the recommended setup: this
# makes the CHARGE atomic, not the admission decision, so a simultaneous
# burst is still possible. What it guarantees is that the burst is repaid
# out of the next window rather than forgiven, which is what keeps the
# long-run rate honest.
class ThrottleAtomicityTest < DispatchPolicy::IntegrationTest
  POLICY = "throttle_atomicity"

  def gate(rate: 10, per: 60)
    DispatchPolicy::Gates::Throttle.new(rate: rate, per: per)
  end

  def seed_partition!(pending: 50)
    DispatchPolicy::Repository.upsert_partition!(
      policy_name: POLICY, partition_key: "k", queue_name: nil,
      context: {}, delta_pending: pending
    )
  end

  def partition_row
    DispatchPolicy::Repository.normalize_partition(
      ActiveRecord::Base.connection.exec_query(
        "SELECT * FROM dispatch_policy_partitions WHERE partition_key = 'k'"
      ).first
    )
  end

  def tokens
    partition_row["gate_state"].dig("throttle", "tokens")
  end

  def admit!(gate, decision, count)
    DispatchPolicy::Repository.record_partition_admit!(
      policy_name:      POLICY,
      partition_key:    "k",
      admitted:         count,
      gate_state_patch: gate.consume(decision, count),
      retry_after:      nil,
      throttle_charge:  decision.charge
    )
  end

  def test_two_ticks_racing_on_one_partition_both_get_charged
    seed_partition!
    g   = gate
    ctx = DispatchPolicy::Context.wrap({})

    # Both read the row before either writes — what happens whenever two
    # loops cover the same shard, since the claim's FOR UPDATE lock is
    # released when that statement ends.
    a = g.evaluate(ctx, partition_row, 100)
    b = g.evaluate(ctx, partition_row, 100)
    assert_equal 10, a.allowed
    assert_equal 10, b.allowed

    admit!(g, a, a.allowed)
    admit!(g, b, b.allowed)

    assert_in_delta(-10.0, tokens, 0.01,
                    "20 jobs left against a bucket of 10: the second charge must land too. " \
                    "Overwriting it (tokens == 0) is what made the rate rate x loops")
  end

  def test_the_overdraft_is_repaid_not_forgiven
    seed_partition!
    g   = gate
    ctx = DispatchPolicy::Context.wrap({})
    a   = g.evaluate(ctx, partition_row, 100)
    b   = g.evaluate(ctx, partition_row, 100)
    admit!(g, a, a.allowed)
    admit!(g, b, b.allowed)

    decision = g.evaluate(ctx, partition_row, 100)
    assert_equal 0, decision.allowed, "a bucket in debt admits nothing"
    assert_operator decision.retry_after, :>, 60.0,
                    "the backoff must cover the debt — more than one window, " \
                    "so the burst comes out of the next window's budget"
  end

  # The other half of the fix. `evaluate` no longer emits a literal
  # gate_state patch, so a deny flushed after a concurrent admission
  # cannot overwrite the charged bucket with an uncharged refill.
  def test_a_deny_flushed_after_an_admit_does_not_erase_the_charge
    seed_partition!
    g   = gate
    ctx = DispatchPolicy::Context.wrap({})

    denied = g.evaluate(ctx, partition_row, 100) # sees a full bucket
    admitted = g.evaluate(ctx, partition_row, 100)
    admit!(g, admitted, 10)
    assert_in_delta 0.0, tokens, 0.01

    refute_includes (denied.gate_state_patch || {}), "throttle",
                    "the refill is recomputable; persisting it is what let a deny " \
                    "undo an admission's cost"

    DispatchPolicy::Repository.bulk_record_partition_denies!([{
      policy_name:      POLICY,
      partition_key:    "k",
      gate_state_patch: denied.gate_state_patch || {},
      retry_after:      1
    }])

    assert_in_delta 0.0, tokens, 0.01,
                    "the deny must leave the charged bucket alone"
  end

  # A bucket already in debt must not be mistaken for one holding tokens:
  # `floor` of a negative is negative, and an `allowed` below zero would
  # sail through as "not empty".
  def test_a_negative_bucket_denies_rather_than_admitting
    seed_partition!
    g = gate
    ActiveRecord::Base.connection.exec_query(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET gate_state = jsonb_build_object('throttle', jsonb_build_object(
            'tokens', -3.0, 'refilled_at', EXTRACT(EPOCH FROM now())))
      WHERE partition_key = 'k'
    SQL

    decision = g.evaluate(DispatchPolicy::Context.wrap({}), partition_row, 100)
    assert_equal 0, decision.allowed
    refute_nil decision.retry_after, "a bucket in debt must back off, not spin"
    # Owes 3, needs 1 more to admit: 4 tokens at 10 per 60s = 24s. The
    # delta absorbs the sliver of refill between the row's timestamp and
    # this evaluate.
    assert_in_delta 24.0, decision.retry_after, 0.5
  end
end
