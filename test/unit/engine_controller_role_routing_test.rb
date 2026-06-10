# frozen_string_literal: true

require_relative "../test_helper"
require "action_controller"
require_relative "../../app/controllers/dispatch_policy/application_controller"

# M9: the engine controllers query the gem tables through the AR models
# directly (Partition, PolicySetting, …), which the Repository role wrapper
# doesn't cover. The around_action must route every action — including view
# rendering — through config.database_role, or under multi-DB the whole
# dashboard reads the default writing role (where the gem tables don't
# live) and 500s.
class EngineControllerRoleRoutingTest < Minitest::Test
  def teardown
    DispatchPolicy.reset_config!
  end

  def test_route_database_role_is_registered_as_an_around_action
    filters = DispatchPolicy::ApplicationController
              ._process_action_callbacks
              .select { |cb| cb.kind == :around }
              .map(&:filter)

    assert_includes filters, :route_database_role,
                    "every engine action must run inside the configured database role"
  end

  def test_route_database_role_opens_the_configured_role
    DispatchPolicy.config.database_role = :queue

    captured_roles = []
    connected_to   = lambda do |role:, &blk|
      captured_roles << role
      blk.call
    end

    ran = false
    ActiveRecord::Base.stub(:connected_to, connected_to) do
      DispatchPolicy::ApplicationController.new.send(:route_database_role) { ran = true }
    end

    assert ran, "the wrapped action must still execute"
    assert_equal [:queue], captured_roles
  end

  def test_route_database_role_without_role_is_a_passthrough
    DispatchPolicy.config.database_role = nil

    called = false
    ran    = false
    ActiveRecord::Base.stub(:connected_to, ->(**) { called = true }) do
      DispatchPolicy::ApplicationController.new.send(:route_database_role) { ran = true }
    end

    assert ran
    refute called, "with no database_role, connected_to must not be invoked"
  end
end
