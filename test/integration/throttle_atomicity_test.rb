# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

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

  # For the cases that have to go through the real Tick.
  class TickedJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("throttle_atomicity_ticked") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 10, per: 60
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(TickedJob::POLICY, owner: TickedJob.name)
  end

  def teardown
    DispatchPolicy.config.clock = -> { Time.now.utc }
    super
  end

  def ticked_row
    DispatchPolicy::Repository.normalize_partition(
      ActiveRecord::Base.connection.exec_query(
        "SELECT * FROM dispatch_policy_partitions WHERE policy_name = 'throttle_atomicity_ticked'"
      ).first
    )
  end

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

  # The whole point of settling in SQL is that the number comes from the
  # ROW, not from whatever Ruby read earlier. A charge carrying a token
  # count that disagrees with the row must not be able to influence the
  # result — that value exists only for the in-memory mirror #consume
  # hands back for the tick's second pass.
  def test_the_persisted_bucket_comes_from_the_row_not_from_the_charge
    seed_partition!
    g        = gate
    decision = g.evaluate(DispatchPolicy::Context.wrap({}), partition_row, 100)
    lying    = decision.charge.merge(tokens: 999.0)

    DispatchPolicy::Repository.record_partition_admit!(
      policy_name: POLICY, partition_key: "k", admitted: 4,
      gate_state_patch: {}, retry_after: nil,
      throttle_charge: lying
    )

    assert_in_delta 6.0, tokens, 0.01,
                    "10 in the row minus 4 admitted — the charge's own token count is not an input"
  end

  # The refill term and the capacity clamp are the load-bearing half of
  # the charge SQL: neutralise either and the suite must go red. Without
  # the refill a partition's bucket only ever falls; without the clamp a
  # long-idle one banks unbounded tokens and bursts far above `rate`.
  def test_the_refill_is_computed_in_sql_from_the_rows_own_timestamp
    seed_partition!
    now = DispatchPolicy.config.now.to_f
    write_bucket!(tokens: 0.0, refilled_at: now - 30) # 30s at 10/60 = 5 tokens

    charge!(admitted: 2, now: now)

    assert_in_delta 3.0, tokens, 0.05,
                    "0 + 30s of refill = 5, minus 2 admitted"
  end

  def test_the_refill_is_clamped_to_capacity
    seed_partition!
    now = DispatchPolicy.config.now.to_f
    write_bucket!(tokens: 0.0, refilled_at: now - 1_000_000)

    charge!(admitted: 3, now: now)

    assert_in_delta 7.0, tokens, 0.05,
                    "an idle century still only buys a full bucket: 10 - 3"
  end

  # The bucket is read by `evaluate` on DispatchPolicy.config.now and must
  # therefore be written on the same clock. Settling it against Postgres
  # `now()` instead puts the two ends of one subtraction on two clocks:
  # any offset is credited as free tokens on EVERY evaluate, forever. And
  # `now()` is the transaction timestamp, so an enclosing transaction
  # freezes it while the app clock keeps moving.
  def test_the_bucket_is_read_and_written_on_one_clock
    DispatchPolicy.config.clock = -> { Time.now.utc + 3600 }
    30.times { TickedJob.perform_later }

    DispatchPolicy::Tick.run(policy_name: "throttle_atomicity_ticked")
    assert_equal 20, DispatchPolicy::StagedJob.count, "one bucket's worth left"

    # Clear the gate's backoff so the next tick really re-evaluates.
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_partitions SET next_eligible_at = NULL"
    )
    DispatchPolicy::Tick.run(policy_name: "throttle_atomicity_ticked")

    assert_equal 20, DispatchPolicy::StagedJob.count,
                 "no real time passed, so no refill — an app clock an hour ahead of " \
                 "the database is not a token supply"
  end

  # Two admission transactions can execute in the opposite order to the
  # one they started in, and `now()` is fixed at transaction start — so a
  # naive stamp moves BACKWARDS and the interval between them is refilled
  # twice. The stamp is monotonic instead. Going through the real Tick
  # also pins the wiring: settle this in Ruby rather than in the UPDATE
  # and the stamp is rewound to the reader's clock.
  def test_the_refill_stamp_never_moves_backwards
    3.times { TickedJob.perform_later }
    future = DispatchPolicy.config.now.to_f + 30
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET gate_state = jsonb_build_object('throttle', jsonb_build_object(
            'tokens', 10.0, 'refilled_at', #{future}))
      WHERE policy_name = 'throttle_atomicity_ticked'
    SQL

    DispatchPolicy::Tick.run(policy_name: "throttle_atomicity_ticked")

    assert_in_delta future,
                    ticked_row["gate_state"].dig("throttle", "refilled_at").to_f, 0.01,
                    "rewinding the stamp re-credits the interval it moved back over"
    assert_in_delta 7.0, ticked_row["gate_state"].dig("throttle", "tokens").to_f, 0.01
  end

  private

  def write_bucket!(tokens:, refilled_at:)
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET gate_state = jsonb_build_object('throttle', jsonb_build_object(
            'tokens', #{tokens}, 'refilled_at', #{refilled_at}))
      WHERE partition_key = 'k' AND policy_name = '#{POLICY}'
    SQL
  end

  def charge!(admitted:, now:)
    DispatchPolicy::Repository.record_partition_admit!(
      policy_name: POLICY, partition_key: "k", admitted: admitted,
      gate_state_patch: {}, retry_after: nil,
      throttle_charge: { capacity: 10.0, refill_rate: 10.0 / 60, tokens: 0.0, now: now }
    )
  end
end
