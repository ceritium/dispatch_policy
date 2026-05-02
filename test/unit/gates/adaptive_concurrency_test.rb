# frozen_string_literal: true

require_relative "../../test_helper"

# Unit tests for the gate's evaluate path. Stubs Repository so they
# stay DB-less; the AIMD math itself runs in the real Postgres UPDATE
# and is exercised by test/integration/adaptive_concurrency_test.rb.
class AdaptiveConcurrencyGateTest < Minitest::Test
  Gate = DispatchPolicy::Gates::AdaptiveConcurrency

  def setup
    super
    DispatchPolicy.reset_registry!
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build("p") do
        partition_by ->(_c) { "k" }
        gate :adaptive_concurrency, initial_max: 4, target_lag_ms: 1000
      end
    )
    @stubs = {}
  end

  def teardown
    restore_repo_stubs!
    DispatchPolicy.reset_registry!
  end

  def stub_repo(method_name, &block)
    repo = DispatchPolicy::Repository.singleton_class
    @stubs[method_name] ||= repo.instance_method(method_name)
    repo.define_method(method_name, &block)
  end

  def restore_repo_stubs!
    repo = DispatchPolicy::Repository.singleton_class
    @stubs.each { |m, original| repo.define_method(m, original) }
    @stubs.clear
  end

  def partition
    { "policy_name" => "p", "partition_key" => "k", "gate_state" => {} }
  end

  def make_gate(**opts)
    Gate.new(**{ initial_max: 4, target_lag_ms: 1000 }.merge(opts))
  end

  # ----- evaluate -----------------------------------------------------------

  def test_first_admission_seeds_and_uses_initial_max
    seeded = nil
    stub_repo(:adaptive_seed!)        { |**kw| seeded = kw }
    stub_repo(:adaptive_current_max)  { |**| nil } # no row yet
    stub_repo(:count_inflight)        { |**| 0 }

    d = make_gate(initial_max: 6).evaluate(DispatchPolicy::Context.wrap({}), partition, 100)

    assert_equal 6, d.allowed
    refute_nil seeded
    assert_equal 6, seeded[:initial_max]
  end

  def test_admits_up_to_current_max_minus_inflight
    stub_repo(:adaptive_seed!)       { |**| }
    stub_repo(:adaptive_current_max) { |**| 5 }
    stub_repo(:count_inflight)       { |**| 2 }

    d = make_gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 3, d.allowed
  end

  def test_full_returns_zero_with_retry_after
    stub_repo(:adaptive_seed!)       { |**| }
    stub_repo(:adaptive_current_max) { |**| 5 }
    # full → in_flight == cap, but in_flight is also > 0 so no safety
    # valve kicks in
    stub_repo(:count_inflight)       { |**| 5 }

    d = make_gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 0, d.allowed
    assert d.retry_after.positive?
    assert_equal "adaptive_concurrency_full", d.reason
  end

  def test_safety_valve_when_inflight_zero_uses_initial_max_floor
    # current_max has been shrunk to 1 by past slow observations; now
    # the partition is idle with 0 in-flight. We must not stay stuck
    # at 1 — admit at least initial_max so observations can flow.
    stub_repo(:adaptive_seed!)       { |**| }
    stub_repo(:adaptive_current_max) { |**| 1 }
    stub_repo(:count_inflight)       { |**| 0 }

    d = make_gate(initial_max: 4, min: 1).evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 4, d.allowed,
                 "with no in-flight the gate must hand at least initial_max so the cap can re-grow"
  end

  def test_admit_budget_caps_allowed
    stub_repo(:adaptive_seed!)       { |**| }
    stub_repo(:adaptive_current_max) { |**| 100 }
    stub_repo(:count_inflight)       { |**| 0 }

    d = make_gate(initial_max: 100).evaluate(DispatchPolicy::Context.wrap({}), partition, 7)
    assert_equal 7, d.allowed
  end

  def test_min_floor_clamps_current_max_when_db_returns_lower
    # The gate's @min should clamp on read too, in case the DB row
    # somehow holds a value below min (shouldn't happen, but defensive).
    stub_repo(:adaptive_seed!)       { |**| }
    stub_repo(:adaptive_current_max) { |**| 0 }
    stub_repo(:count_inflight)       { |**| 5 } # in_flight > 0 so no safety valve

    d = make_gate(initial_max: 4, min: 2).evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    # cap clamped to min=2; in_flight=5; remaining = -3 → denied
    assert_equal 0, d.allowed
  end

  # ----- record_observation -------------------------------------------------

  def test_record_observation_seeds_and_calls_repository
    seed_called   = false
    record_args   = nil
    stub_repo(:adaptive_seed!)   { |**| seed_called = true }
    stub_repo(:adaptive_record!) { |**kw| record_args = kw }

    make_gate(initial_max: 8, target_lag_ms: 500, min: 2)
      .record_observation(policy_name: "p", partition_key: "k", queue_lag_ms: 250, succeeded: true)

    assert seed_called
    assert_equal "p",  record_args[:policy_name]
    assert_equal "k",  record_args[:partition_key]
    assert_equal 250.0, record_args[:queue_lag_ms]
    assert_equal true,  record_args[:succeeded]
    assert_equal 500.0, record_args[:target_lag_ms]
    assert_equal 2,     record_args[:min]
  end

  # ----- construction -------------------------------------------------------

  def test_construction_validates_target_lag_ms
    assert_raises(ArgumentError) { Gate.new(initial_max: 1, target_lag_ms: 0) }
    assert_raises(ArgumentError) { Gate.new(initial_max: 1, target_lag_ms: -1) }
  end

  def test_construction_validates_min_and_initial_max
    assert_raises(ArgumentError) { Gate.new(initial_max: 1, target_lag_ms: 1000, min: 0) }
    assert_raises(ArgumentError) { Gate.new(initial_max: 1, target_lag_ms: 1000, min: 5) }
  end
end
