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

  def test_default_shard_when_no_shard_by_declared
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
    end
    ctx = policy.build_context([])
    assert_equal "default", policy.shard_for(ctx)
  end

  def test_shard_by_proc_can_use_queue_name_from_enriched_context
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
      shard_by ->(c) { c[:queue_name] }
    end

    ctx = policy.build_context([], queue_name: "payments")
    assert_equal "payments", ctx[:queue_name]
    assert_equal "payments", policy.shard_for(ctx)
  end

  def test_shard_by_falls_back_to_default_when_proc_returns_nil
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
      shard_by ->(_c) { nil }
    end
    assert_equal "default", policy.shard_for(policy.build_context([]))
  end

  # ----- policy-level partition_by ----------------------------------------

  def test_policy_level_partition_by_replaces_concatenation
    policy = DispatchPolicy::PolicyDSL.build("p") do
      context ->(args) { { tenant: args.first } }
      partition_by ->(c) { "tenant:#{c[:tenant]}" }
      gate :throttle,    rate: 10, per: 60
      gate :concurrency, max:  5
    end

    ctx = policy.build_context(["acme"])
    assert_equal "tenant:acme", policy.partition_key_for(ctx),
                 "policy-level partition_by must produce a single canonical key"
    assert_equal "tenant:acme", policy.partition_for(ctx)
  end

  def test_policy_level_partition_by_winning_emits_warning_when_gates_also_set_one
    captured = StringIO.new
    DispatchPolicy.config.logger = Logger.new(captured)

    DispatchPolicy::PolicyDSL.build("p") do
      partition_by    ->(c) { c[:tenant] }
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "ignored" }
    end

    assert_match(/policy-level value wins/, captured.string)
  ensure
    DispatchPolicy.reset_config!
  end

  def test_policy_level_partition_by_used_for_concurrency_inflight_key
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

  def test_partition_for_returns_nil_when_policy_level_partition_by_unset
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
    end
    assert_nil policy.partition_for(policy.build_context([]))
  end
end
