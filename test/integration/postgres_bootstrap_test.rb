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

  # A drift check that never says "current" is not a safe default: it is a
  # silent DROP CASCADE and full re-migrate before EVERY integration case,
  # which is most of the suite's wall time and looks like nothing at all.
  # That is what happened when `defaults_current?` matched the default
  # expression as WRITTEN (`AT TIME ZONE 'UTC'`) rather than as Postgres
  # stores it (`timezone('UTC'::text, …)`).
  #
  # A mutation cannot catch this on its own — always-rebuild is slow, not
  # wrong, so the suite stays green. The property has to be asserted.
  def test_a_freshly_built_schema_reports_as_current
    skip "no Postgres available" unless PG.connect!
    PG.ensure_schema!

    assert PG.schema_present?,
           "the schema was just built, so the drift check must say it is current — " \
           "otherwise every integration case silently rebuilds it"
  end

  # And it must still notice a default that is genuinely wrong, or it is
  # only fast.
  def test_a_stale_timestamp_default_reports_as_drifted
    skip "no Postgres available" unless PG.connect!
    PG.ensure_schema!
    conn = ActiveRecord::Base.connection
    conn.execute("ALTER TABLE dispatch_policy_tick_samples ALTER COLUMN sampled_at SET DEFAULT now()")

    refute PG.schema_present?, "a default reverted to the session clock is drift"
  ensure
    PG.ensure_schema! if PG.connect!
  end
end
