# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"

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
  # The gem's own AR models hang off ActiveRecord::Base, so once the role
  # swap stopped going through the global hierarchy they stayed on the
  # host's primary — on exactly the multi-database install
  # `database_connection_class` exists for. `record_sample!` then raises
  # into its own rescue (tick samples empty forever, one WARN per tick)
  # and every dashboard page 500s inside the around_action that is
  # supposed to route it.
  class OtherBase < ActiveRecord::Base
    self.abstract_class = true
    def self.connection_specification_name = "OtherBaseSpec"
  end

  def test_the_engines_models_follow_the_configured_connection_class
    was = DispatchPolicy::ApplicationRecord.connection_specification_name
    DispatchPolicy.config.database_connection_class = OtherBase

    DispatchPolicy.route_models_to_configured_connection!

    assert_equal "OtherBaseSpec",
                 DispatchPolicy::ApplicationRecord.connection_specification_name,
                 "left on ActiveRecord::Base they look for the gem's tables on the " \
                 "host's primary, where they do not exist"
  ensure
    DispatchPolicy::ApplicationRecord.connection_specification_name = was
  end

  def test_it_is_a_no_op_on_a_single_database
    was = DispatchPolicy::ApplicationRecord.connection_specification_name
    DispatchPolicy.config.database_connection_class = nil

    DispatchPolicy.route_models_to_configured_connection!

    assert_equal was, DispatchPolicy::ApplicationRecord.connection_specification_name
  end
  # The behavioural test above calls the method directly, because the test
  # environment does not boot Rails — so pin the wiring separately, or
  # deleting the railtie's call leaves the suite green while the models go
  # back to the wrong database.
  # Pinned to `to_prepare` specifically, and asserted that way. The
  # engine's models are in the host's RELOADABLE autoloader, so a hook
  # that fires once — after_initialize — is undone by the first code
  # reload in development and the models go back to the host's primary.
  # An earlier version of this test asserted the call was inside the
  # after_initialize block, which meant the correct fix turned the suite
  # red: a test can pin the wrong half in either direction.
  def test_the_railtie_routes_the_models_on_every_reload
    source = File.read(File.expand_path("../../lib/dispatch_policy/railtie.rb", __dir__))

    prepare = source[/config\.to_prepare do.*?\n    end/m]
    refute_nil prepare, "after_initialize fires once; the models are reloadable"
    assert_includes prepare, "route_models_to_configured_connection!"

    boot = source[/config\.after_initialize do.*?\n    end/m]
    assert_includes boot, "warn_unsupported_adapter"
  end
end
