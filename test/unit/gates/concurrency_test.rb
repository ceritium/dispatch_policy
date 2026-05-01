# frozen_string_literal: true

require_relative "../../test_helper"

class ConcurrencyGateTest < Minitest::Test
  def stub_repo_count(value)
    DispatchPolicy::Repository.singleton_class.send(:alias_method, :__orig_count, :count_inflight) unless DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_count)
    DispatchPolicy::Repository.singleton_class.define_method(:count_inflight) { |**| value }
  end

  def teardown
    if DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_count)
      DispatchPolicy::Repository.singleton_class.send(:alias_method, :count_inflight, :__orig_count)
    end
  end

  def partition
    { "policy_name" => "p", "partition_key" => "k", "gate_state" => {} }
  end

  def test_admits_up_to_remaining
    stub_repo_count(2)
    gate = DispatchPolicy::Gates::Concurrency.new(max: 5, partition_by: ->(_c) { "acct:1" })
    d = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 3, d.allowed
  end

  def test_full_returns_zero
    stub_repo_count(5)
    gate = DispatchPolicy::Gates::Concurrency.new(max: 5, partition_by: ->(_c) { "acct:1" })
    d = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 0, d.allowed
  end

  def test_dynamic_max_changes_between_calls
    stub_repo_count(0)
    gate = DispatchPolicy::Gates::Concurrency.new(max: ->(c) { c[:max] }, partition_by: ->(_c) { "k" })

    d1 = gate.evaluate(DispatchPolicy::Context.wrap({ max: 3 }), partition, 100)
    assert_equal 3, d1.allowed

    # Same gate, same partition row in the table — but ctx changed (e.g. operator
    # bumped a DB attribute and a new perform_later refreshed partitions.context).
    d2 = gate.evaluate(DispatchPolicy::Context.wrap({ max: 7 }), partition, 100)
    assert_equal 7, d2.allowed
  end

  def test_admit_budget_caps_allowed
    stub_repo_count(0)
    gate = DispatchPolicy::Gates::Concurrency.new(max: 100, partition_by: ->(_c) { "k" })
    d = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 5)
    assert_equal 5, d.allowed
  end

  def test_zero_max_denies
    stub_repo_count(0)
    gate = DispatchPolicy::Gates::Concurrency.new(max: 0, partition_by: ->(_c) { "k" })
    d = gate.evaluate(DispatchPolicy::Context.wrap({}), partition, 100)
    assert_equal 0, d.allowed
  end
end
