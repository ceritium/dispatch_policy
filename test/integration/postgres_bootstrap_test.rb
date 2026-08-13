# frozen_string_literal: true

require_relative "../test_helper"

# The shared bootstrap memoizes a SUCCESSFUL connection so ten test classes
# don't open it independently. It must not memoize a failure: one transient
# hiccup — Postgres restarting, a momentary connection limit — would
# otherwise skip every remaining integration case for the rest of the run,
# and outside CI (where DISPATCH_POLICY_REQUIRE_DB makes a missing database
# fatal) the suite would report green having exercised only unit tests.
#
# Deliberately NOT a DispatchPolicy::IntegrationTest subclass: it drives
# PostgresTest.connect! itself, so it must not run inside the base class's
# setup, which calls it.
class PostgresBootstrapTest < Minitest::Test
  PG = DispatchPolicy::PostgresTest

  def setup
    super
    @saved_connected = PG.instance_variable_get(:@connected)
    @saved_warned    = PG.instance_variable_get(:@warned)
    @saved_db_name   = ENV["DB_NAME"]
    @saved_require   = ENV["DISPATCH_POLICY_REQUIRE_DB"]
  end

  def teardown
    ENV["DB_NAME"] = @saved_db_name
    ENV["DISPATCH_POLICY_REQUIRE_DB"] = @saved_require
    PG.instance_variable_set(:@connected, @saved_connected)
    PG.instance_variable_set(:@warned, @saved_warned)
    # Leave the process pointed back at the real database: the failure
    # probe below re-establishes the connection against a missing one.
    PG.connect!
    super
  end

  def test_a_failed_connect_does_not_disable_every_later_case
    skip "no Postgres available" unless PG.connect!

    # Force the next call to actually attempt a connection.
    PG.instance_variable_set(:@connected, nil)
    # The probe deliberately fails; the CI guard would turn that into a
    # raise, which is right for a real run and wrong for this test.
    ENV.delete("DISPATCH_POLICY_REQUIRE_DB")
    ENV["DB_NAME"] = "dispatch_policy_absent_#{Process.pid}"

    refute PG.connect!, "a missing database must report failure"

    ENV["DB_NAME"] = @saved_db_name
    assert PG.connect!,
           "the next test class must retry — caching the failure would skip " \
           "every remaining integration case and still report a green run"
  end
end
