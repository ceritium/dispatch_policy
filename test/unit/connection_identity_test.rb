# frozen_string_literal: true

require_relative "../test_helper"

# The gem's guarantee is that the adapter's INSERT joins the admission
# transaction, which only holds while both are on ONE connection. So the
# gem has to open its transaction on the class the ADAPTER writes
# through — not on ActiveRecord::Base and not on a global role swap.
#
# `ActiveRecord::Base.connected_to(role:)` moves every class in that
# hierarchy, the host's own models included, for the duration of the
# block; on the documented separate-queue-database install that put the
# whole process on the queue database while the adapter still wrote
# through its own class on its own connection.
class ConnectionIdentityTest < Minitest::Test
  class FakeRecord
    def self.connected_to(role:)
      Calls.roles << [name, role]
      yield
    end

    def self.name = "FakeAdapter::Record"
  end

  module Calls
    def self.roles = (@roles ||= [])
    def self.reset! = @roles = []
  end

  def setup
    super
    Calls.reset!
  end

  def teardown
    DispatchPolicy.reset_config!
  end

  def test_base_class_defaults_to_active_record_base
    assert_equal ActiveRecord::Base, DispatchPolicy::Repository.base_class
  end

  def test_base_class_resolves_a_configured_string
    DispatchPolicy.config.database_connection_class = "ConnectionIdentityTest::FakeRecord"
    assert_equal FakeRecord, DispatchPolicy::Repository.base_class
  end

  def test_the_role_swap_is_scoped_to_the_configured_class
    DispatchPolicy.config.database_connection_class = FakeRecord
    DispatchPolicy.config.database_role = :queue

    DispatchPolicy::Repository.with_connection { :done }

    assert_equal [["FakeAdapter::Record", :queue]], Calls.roles,
                 "opening the role on ActiveRecord::Base moves the host's models too, " \
                 "and still leaves the adapter writing on another connection"
  end

  def test_no_role_configured_still_yields_without_a_swap
    DispatchPolicy.config.database_connection_class = FakeRecord
    DispatchPolicy.config.database_role = nil

    assert_equal :done, DispatchPolicy::Repository.with_connection { :done }
    assert_empty Calls.roles
  end
end
