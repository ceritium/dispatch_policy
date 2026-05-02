# frozen_string_literal: true

require_relative "../test_helper"

class PolicyDSLTest < Minitest::Test
  def test_builds_policy_with_gates_and_context
    policy = DispatchPolicy::PolicyDSL.build("endpoints") do
      context ->(args) {
        event = args.first
        { endpoint_id: event[:endpoint_id], rate_limit: event[:rate_limit] }
      }

      partition_by ->(c) { c[:endpoint_id] }

      gate :throttle,
           rate: ->(c) { c[:rate_limit] },
           per:  60

      gate :concurrency,
           max: ->(c) { c[:max_per_account] || 5 }

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

  def test_partition_key_uses_policy_partition_by
    policy = DispatchPolicy::PolicyDSL.build("p") do
      context ->(args) { { ep: args.first } }
      partition_by ->(c) { "ep:#{c[:ep]}" }
      gate :throttle,    rate: 5, per: 60
      gate :concurrency, max: 3
    end

    ctx = policy.build_context(["foo"])
    assert_equal "ep:foo", policy.partition_key_for(ctx),
                 "the staged partition_key is the policy's canonical partition value"
    assert_equal "ep:foo", policy.partition_for(ctx)
  end

  def test_unknown_gate_raises
    assert_raises(DispatchPolicy::UnknownGate) do
      DispatchPolicy::PolicyDSL.build("p") do
        partition_by ->(_c) { "k" }
        gate :nope, foo: 1
      end
    end
  end

  def test_invalid_retry_strategy_raises
    assert_raises(DispatchPolicy::InvalidPolicy) do
      DispatchPolicy::PolicyDSL.build("p") do
        partition_by ->(_c) { "k" }
        gate :throttle, rate: 1, per: 1
        retry_strategy :foo
      end
    end
  end

  def test_no_gates_is_allowed_for_fairness_only_policies
    # A policy with no gates uses admission_batch_size (or
    # tick_admission_budget) as its ceiling and relies on the in-tick
    # fairness reorder to distribute admissions across partitions.
    # Valid for "balance N tenants without rate-limiting any of them".
    policy = DispatchPolicy::PolicyDSL.build("fair_only") do
      partition_by ->(c) { c[:tenant] }
    end
    assert_empty policy.gates
    assert_equal "fair_only", policy.name
  end

  def test_missing_partition_by_raises
    err = assert_raises(DispatchPolicy::InvalidPolicy) do
      DispatchPolicy::PolicyDSL.build("p") do
        gate :throttle, rate: 1, per: 60
      end
    end
    assert_match(/partition_by required/, err.message)
  end

  def test_default_shard_when_no_shard_by_declared
    policy = DispatchPolicy::PolicyDSL.build("p") do
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 1, per: 60
    end
    ctx = policy.build_context([])
    assert_equal "default", policy.shard_for(ctx)
  end

  def test_shard_by_proc_can_use_queue_name_from_enriched_context
    policy = DispatchPolicy::PolicyDSL.build("p") do
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 1, per: 60
      shard_by ->(c) { c[:queue_name] }
    end

    ctx = policy.build_context([], queue_name: "payments")
    assert_equal "payments", ctx[:queue_name]
    assert_equal "payments", policy.shard_for(ctx)
  end

  def test_shard_by_falls_back_to_default_when_proc_returns_nil
    policy = DispatchPolicy::PolicyDSL.build("p") do
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 1, per: 60
      shard_by ->(_c) { nil }
    end
    assert_equal "default", policy.shard_for(policy.build_context([]))
  end

  # ----- policy-level partition_by canonical scope ------------------------

  def test_policy_partition_by_used_for_concurrency_inflight_key
    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build("p") do
      context ->(args) { { tenant: args.first } }
      partition_by ->(c) { "tenant:#{c[:tenant]}" }
      gate :concurrency, max: 5
    end
    DispatchPolicy.registry.register(policy)

    concurrency = policy.gates.find { |g| g.name == :concurrency }
    ctx = policy.build_context(["acme"])
    # Inflight key matches the staged partition_key — same canonical scope.
    assert_equal "tenant:acme", concurrency.inflight_partition_key("p", ctx)
  ensure
    DispatchPolicy.reset_registry!
  end

  def test_partition_for_returns_nil_partition_value_when_proc_returns_nil
    policy = DispatchPolicy::PolicyDSL.build("p") do
      partition_by ->(_c) { nil }
      gate :throttle, rate: 1, per: 60
    end
    assert_equal "", policy.partition_for(policy.build_context([])),
                 "nil partition_by result is coerced to empty string"
  end
end
