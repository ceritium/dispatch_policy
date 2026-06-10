# frozen_string_literal: true

require_relative "../test_helper"

class RegistryTest < Minitest::Test
  def build_policy(name)
    DispatchPolicy::PolicyDSL.build(name) do
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 1, per: 60
    end
  end

  def test_register_fetch_names_size
    reg = DispatchPolicy::Registry.new
    reg.register(build_policy("a"))
    reg.register(build_policy("b"))

    assert_equal 2, reg.size
    assert_equal %w[a b], reg.names.sort
    assert_equal "a", reg.fetch("a").name
    assert_nil reg.fetch("missing")
  end

  def test_each_snapshots_and_iterates_outside_the_lock
    reg = DispatchPolicy::Registry.new
    reg.register(build_policy("a"))
    # The block calls back into the registry; with `each` holding the
    # (non-reentrant) mutex while yielding this would deadlock.
    seen = []
    reg.each { |p| seen << [p.name, reg.size] }
    assert_equal [["a", 1]], seen
  end

  # L10: reads now take the same mutex as writes. A read concurrent with a
  # write must never raise or observe a torn Hash.
  def test_concurrent_reads_and_writes_are_safe
    reg = DispatchPolicy::Registry.new
    errors = Queue.new

    writers = 4.times.map do |w|
      Thread.new do
        50.times { |i| reg.register(build_policy("p#{w}-#{i}")) }
      rescue StandardError => e
        errors << e
      end
    end

    readers = 4.times.map do
      Thread.new do
        200.times do
          reg.names
          reg.size
          reg.each { |_p| }
        end
      rescue StandardError => e
        errors << e
      end
    end

    (writers + readers).each(&:join)
    assert errors.empty?, "concurrent registry access raised: #{errors.size > 0 ? errors.pop : ''}"
    assert_equal 200, reg.size
  end
end
