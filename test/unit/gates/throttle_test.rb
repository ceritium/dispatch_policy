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
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60, partition_by: ->(_c) { "x" })
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)

    assert_equal 10, decision.allowed
    assert_in_delta 0.0, decision.gate_state_patch.dig("throttle", "tokens"), 0.001
  end

  def test_admit_budget_caps_allowed
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60, partition_by: ->(_c) { "x" })
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 3)

    assert_equal 3, decision.allowed
    assert_in_delta 7.0, decision.gate_state_patch.dig("throttle", "tokens"), 0.001
  end

  def test_no_tokens_returns_retry_after
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60, partition_by: ->(_c) { "x" })

    state = { "gate_state" => { "throttle" => { "tokens" => 0.0, "refilled_at" => @clock_now.to_f } } }
    partition = empty_partition.merge(state)

    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)

    assert_equal 0, decision.allowed
    refresh_per_token = 60.0 / 10.0
    assert_in_delta refresh_per_token, decision.retry_after, 0.05
  end

  def test_refill_after_elapsed_time
    gate = DispatchPolicy::Gates::Throttle.new(rate: 10, per: 60, partition_by: ->(_c) { "x" })

    state = { "gate_state" => { "throttle" => { "tokens" => 0.0, "refilled_at" => @clock_now.to_f } } }
    partition = empty_partition.merge(state)

    advance(30)  # 30 / 6 = 5 tokens refilled
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 5, decision.allowed
  end

  def test_dynamic_rate_uses_ctx_value
    gate = DispatchPolicy::Gates::Throttle.new(
      rate: ->(c) { c[:rate_limit] },
      per:  60,
      partition_by: ->(_c) { "x" }
    )

    decision = gate.evaluate(
      DispatchPolicy::Context.wrap({ rate_limit: 4 }),
      empty_partition,
      100
    )
    assert_equal 4, decision.allowed
  end

  def test_zero_rate_denies
    gate = DispatchPolicy::Gates::Throttle.new(rate: 0, per: 60, partition_by: ->(_c) { "x" })
    decision = gate.evaluate(DispatchPolicy::Context.wrap({}), empty_partition, 100)
    assert_equal 0, decision.allowed
  end
end
