# frozen_string_literal: true

require_relative "../../test_helper"

class ThrottleGateTest < Minitest::Test
  def setup
    super
    @t0 = Time.utc(2026, 1, 1, 12, 0, 0)
    DispatchPolicy.config.clock = -> { @clock_now }
    @clock_now = @t0
  end

  def advance(seconds)
    @clock_now = @clock_now + seconds
  end

  def empty_partition
    {
      "policy_name"   => "p",
      "partition_key" => "k",
      "gate_state"    => {}
    }
  end

  def test_first_call_admits_full_capacity
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)

    assert_equal 10, decision.allowed
    # evaluate records the post-refill bucket WITHOUT deducting; the
    # deduction happens in #consume once the real admitted count is known.
    assert_in_delta 10.0, decision.gate_state_patch.dig("throttle", "tokens"), 0.001
  end

  def test_admit_budget_caps_allowed
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 3)

    assert_equal 3, decision.allowed
    # Still the full refilled bucket — admit_budget caps `allowed`, not the
    # recorded token count.
    assert_in_delta 10.0, decision.gate_state_patch.dig("throttle", "tokens"), 0.001
  end

  def test_consume_deducts_exactly_the_admitted_count
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 10, decision.allowed

    # Admitted fewer than allowed (e.g. only 4 staged rows were claimable):
    # the bucket is charged 4, not 10.
    patch = gate.consume(decision, 4)
    assert_in_delta 6.0, patch.dig("throttle", "tokens"), 0.001
    assert_equal decision.gate_state_patch.dig("throttle", "refilled_at"),
                 patch.dig("throttle", "refilled_at")
  end

  def test_consume_with_zero_admits_leaves_the_refilled_bucket_untouched
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)

    # No rows actually claimed (all future-scheduled / raced away): the
    # refill stands, but not a single token is spent.
    patch = gate.consume(decision, 0)
    assert_in_delta 10.0, patch.dig("throttle", "tokens"), 0.001
  end

  def test_no_tokens_returns_retry_after
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)

    state = { "gate_state" => { "throttle" => { "tokens" => 0.0, "refilled_at" => @clock_now.to_f } } }
    partition = empty_partition.merge(state)

    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)

    assert_equal 0, decision.allowed
    refresh_per_token = 60.0 / 10.0
    assert_in_delta refresh_per_token, decision.retry_after, 0.05
  end

  def test_refill_after_elapsed_time
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60)

    state = { "gate_state" => { "throttle" => { "tokens" => 0.0, "refilled_at" => @clock_now.to_f } } }
    partition = empty_partition.merge(state)

    advance(30)  # 30 / 6 = 5 tokens refilled
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 5, decision.allowed
  end

  def test_dynamic_per_uses_ctx_value
    # rate 30 over a 30s window = refill 1 token/s. After 5s, 5 whole tokens.
    gate = DispatchPolicy::Gates::Throttle.new(
      rate: 30,
      per:  ->(c) { c[:window] }
    )
    state     = { "gate_state" => { "throttle" => { "tokens" => 0.0, "refilled_at" => @clock_now.to_f } } }
    partition = empty_partition.merge(state)

    advance(5)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({ window: 30 }), partition, 100)
    assert_equal 5, decision.allowed
  end

  def test_dynamic_per_zero_raises
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: ->(c) { c[:window] })
    assert_raises(ArgumentError) do
      gate.evaluate(DispatchPolicy::Context.wrap({ window: 0 }), empty_partition, 100)
    end
  end

  def test_dynamic_rate_uses_ctx_value
    gate = DispatchPolicy::Gates::Throttle.new(
      rate: ->(c) { c[:rate_limit] },
      per:  60
    )

    decision = gate.evaluate(
      DispatchPolicy::Context.wrap({ rate_limit: 4 }),
      empty_partition,
      100
    )
    assert_equal 4, decision.allowed
  end

  def test_zero_rate_denies
    gate = DispatchPolicy::Gates::Throttle.new(rate: 0, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 0, decision.allowed
  end

  # M4: a rate<=0 deny must carry a retry_after so the partition backs off
  # for a window instead of being re-claimed and re-evaluated every tick
  # (a NULL retry_after leaves it immediately eligible — a busy-loop).
  def test_zero_rate_backs_off_with_retry_after
    gate = DispatchPolicy::Gates::Throttle.new(rate: 0, per: 60)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 0, decision.allowed
    assert_in_delta 60.0, decision.retry_after, 0.001,
                    "rate=0 must back off one window, not deny with a NULL retry_after"
  end

  # M4: a sub-unit rate used to truncate to 0 (Integer(0.5)) and deny
  # forever. With a Float rate and a capacity floored at one whole token,
  # it admits at the correct long-run pace.
  def test_fractional_sub_unit_rate_still_admits
    gate = DispatchPolicy::Gates::Throttle.new(rate: 0.5, per: 1)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 1, decision.allowed,
                 "rate: 0.5 must accumulate a whole token and admit, not truncate to a permanent deny"
  end

  # M4: a fractional rate >= 1 must keep its fractional part instead of
  # truncating every refill (which would systematically under-admit).
  def test_fractional_rate_preserves_precision_in_bucket
    gate = DispatchPolicy::Gates::Throttle.new(rate: 2.5, per: 1)
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 2, decision.allowed, "2.5 tokens floor to 2 whole admits"
    assert_in_delta 2.5, decision.gate_state_patch.dig("throttle", "tokens"), 0.001,
                    "the 0.5 fractional token must be preserved (Float, not Integer)"
  end

  def test_zero_per_raises_on_construction
    assert_raises(ArgumentError) do
      DispatchPolicy::Gates::Throttle.new(rate: 5, per: 0)
    end
  end
end
