# frozen_string_literal: true

require_relative "../test_helper"

class PipelineTest < Minitest::Test
  class FakeGate < DispatchPolicy::Gate
    attr_reader :evaluations
    def initialize(name:, decision:)
      super(partition_by: ->(_c) { name.to_s })
      @gate_name  = name
      @decision   = decision
      @evaluations = 0
    end
    def name
      @gate_name
    end
    def evaluate(_ctx, _partition, budget)
      @evaluations += 1
      @decision
    end
  end

  def make_policy(gates)
    DispatchPolicy::Policy.new(
      name:         "p",
      context_proc: ->(_a) { {} },
      gates:        gates,
      retry_strategy: :restage
    )
  end

  def test_admit_count_is_minimum_across_gates
    g1 = FakeGate.new(name: :a, decision: DispatchPolicy::Decision.new(allowed: 10))
    g2 = FakeGate.new(name: :b, decision: DispatchPolicy::Decision.new(allowed: 4))
    pipeline = DispatchPolicy::Pipeline.new(make_policy([g1, g2]))

    result = pipeline.call(DispatchPolicy::Context.wrap({}), { "policy_name" => "p" }, 100)

    assert_equal 4, result.admit_count
  end

  def test_short_circuits_after_zero
    g1 = FakeGate.new(name: :a, decision: DispatchPolicy::Decision.new(allowed: 0, retry_after: 5))
    g2 = FakeGate.new(name: :b, decision: DispatchPolicy::Decision.new(allowed: 99))
    pipeline = DispatchPolicy::Pipeline.new(make_policy([g1, g2]))

    result = pipeline.call(DispatchPolicy::Context.wrap({}), { "policy_name" => "p" }, 100)

    assert_equal 0, result.admit_count
    assert_equal 1, g1.evaluations
    assert_equal 0, g2.evaluations
  end

  def test_retry_after_is_minimum
    g1 = FakeGate.new(name: :a, decision: DispatchPolicy::Decision.new(allowed: 0, retry_after: 10))
    g2 = FakeGate.new(name: :b, decision: DispatchPolicy::Decision.new(allowed: 0, retry_after: 3))
    # Even though g2 won't actually run because of short-circuit on 0, the test
    # uses a policy where the first gate allows budget through.
    g_keep = FakeGate.new(name: :a, decision: DispatchPolicy::Decision.new(allowed: 5, retry_after: 10))
    pipeline = DispatchPolicy::Pipeline.new(make_policy([g_keep, g2]))

    result = pipeline.call(DispatchPolicy::Context.wrap({}), { "policy_name" => "p" }, 100)
    assert_equal 0, result.admit_count
    assert_in_delta 3, result.retry_after, 0.001
  end

  def test_gate_state_patches_are_merged
    g1 = FakeGate.new(name: :a, decision: DispatchPolicy::Decision.new(allowed: 5, gate_state_patch: { "throttle" => { "tokens" => 0.5 } }))
    g2 = FakeGate.new(name: :b, decision: DispatchPolicy::Decision.new(allowed: 10))
    pipeline = DispatchPolicy::Pipeline.new(make_policy([g1, g2]))

    result = pipeline.call(DispatchPolicy::Context.wrap({}), { "policy_name" => "p" }, 100)

    assert_equal({ "throttle" => { "tokens" => 0.5 } }, result.gate_state_patch)
  end
end
