# frozen_string_literal: true

require_relative "../test_helper"

# H1: every public Repository SQL method must run against
# config.database_role so multi-DB setups hit the DB the gem tables live
# in. These tests stub the connection so they need no database.
class RepositoryRoleRoutingTest < Minitest::Test
  FakeResult = Struct.new(:rows)

  def teardown
    DispatchPolicy.reset_config!
  end

  def test_calls_route_through_configured_database_role
    DispatchPolicy.config.database_role = :queue

    captured_roles = []
    fake_conn = Object.new
    def fake_conn.exec_query(*) = RepositoryRoleRoutingTest::FakeResult.new([[7]])

    connected_to = lambda do |role:, &blk|
      captured_roles << role
      blk.call
    end

    DispatchPolicy::Repository.stub(:connection, fake_conn) do
      ActiveRecord::Base.stub(:connected_to, connected_to) do
        assert_equal 7, DispatchPolicy::Repository.count_inflight(policy_name: "p", partition_key: "k")
      end
    end

    assert_equal [:queue], captured_roles,
                 "count_inflight must open against config.database_role"
  end

  def test_no_role_configured_skips_connected_to
    DispatchPolicy.config.database_role = nil

    fake_conn = Object.new
    def fake_conn.exec_query(*) = RepositoryRoleRoutingTest::FakeResult.new([[3]])

    called = false
    DispatchPolicy::Repository.stub(:connection, fake_conn) do
      ActiveRecord::Base.stub(:connected_to, ->(**) { called = true }) do
        assert_equal 3, DispatchPolicy::Repository.count_inflight(policy_name: "p", partition_key: "k")
      end
    end

    refute called, "with no database_role, connected_to must not be invoked"
  end

  def test_excluded_helpers_are_not_role_wrapped
    # Pure helpers run inside an already-routed caller; wrapping them would
    # add redundant role swaps in hot per-row loops. Verify they don't even
    # consult the role (no connected_to) so the exclusion stays intact.
    DispatchPolicy.config.database_role = :queue
    called = false
    ActiveRecord::Base.stub(:connected_to, ->(**) { called = true; yield }) do
      assert_equal({ "a" => 1 }, DispatchPolicy::Repository.parse_jsonb('{"a":1}'))
    end
    refute called, "parse_jsonb must not be role-wrapped"
  end
end
