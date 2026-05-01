# frozen_string_literal: true

require_relative "../test_helper"

class PolicyDSLTest < Minitest::Test
  def test_builds_policy_with_gates_and_context
    policy = DispatchPolicy::PolicyDSL.build("endpoints") do
      context ->(args) {
        event = args.first
        { endpoint_id: event[:endpoint_id], rate_limit: event[:rate_limit] }
      }

      gate :throttle,
           rate:         ->(c) { c[:rate_limit] },
           per:          60,
           partition_by: ->(c) { c[:endpoint_id] }

      gate :concurrency,
           max:          ->(c) { c[:max_per_account] || 5 },
           partition_by: ->(c) { "acct:#{c[:account_id]}" }

      retry_strategy :bypass
      queue_name :default
    end

    assert_equal "endpoints", policy.name
    assert_equal 2, policy.gates.size
    assert_equal :throttle,    policy.gates[0].name
    assert_equal :concurrency, policy.gates[1].name
    assert policy.bypass_retries?
    refute policy.restage_retries?
    assert_equal "default", policy.queue_name
  end

  def test_partition_key_concatenates_per_gate_partition_by
    policy = DispatchPolicy::PolicyDSL.build("p") do
      context ->(args) { { ep: args.first, acct: 7 } }
      gate :throttle,    rate: 5, per: 60, partition_by: ->(c) { "ep:#{c[:ep]}" }
      gate :concurrency, max: 3,           partition_by: ->(c) { "acct:#{c[:acct]}" }
    end

    ctx = policy.build_context(["foo"])
    assert_equal "throttle=ep:foo|concurrency=acct:7", policy.partition_key_for(ctx)
  end

  def test_unknown_gate_raises
    assert_raises(DispatchPolicy::UnknownGate) do
      DispatchPolicy::PolicyDSL.build("p") do
        gate :nope, foo: 1
      end
    end
  end

  def test_invalid_retry_strategy_raises
    assert_raises(DispatchPolicy::InvalidPolicy) do
      DispatchPolicy::PolicyDSL.build("p") do
        gate :throttle, rate: 1, per: 1, partition_by: ->(_c) { "k" }
        retry_strategy :foo
      end
    end
  end

  def test_no_gates_raises
    assert_raises(DispatchPolicy::InvalidPolicy) do
      DispatchPolicy::PolicyDSL.build("p")
    end
  end
end
