# frozen_string_literal: true

ENV["RAILS_ENV"] ||= "test"

require "minitest/autorun"
require "active_support/testing/time_helpers"

require "active_record"
require "active_job"
require "active_job/test_helper"

# Activate the inline ActiveJob adapter by default for tests that don't talk to DB.
ActiveJob::Base.queue_adapter = :test

require_relative "../lib/dispatch_policy"

module DispatchPolicy
  module TestHelpers
    def reset_dispatch_policy!
      DispatchPolicy.reset_config!
      DispatchPolicy.reset_registry!
    end
  end

  # Postgres bootstrap shared by every case under test/integration.
  #
  # Each of those files used to carry its own copy: ten byte-identical
  # `self.connect!` blocks, and truncate lists that had drifted apart —
  # five files cleaned four tables, three cleaned five, none cleaned
  # `dispatch_policy_policy_settings`. A test that leaves a paused policy
  # behind therefore makes a LATER test's `claim_partitions` return
  # nothing, and it fails somewhere else entirely.
  #
  # Worse, only three of the ten created the schema. On a fresh database
  # whether the suite passed depended on which class Minitest's random
  # seed happened to run first — `dropdb && createdb` + `rake test` went
  # from green to 17 errors and back to green on a re-run, purely on
  # ordering.
  module PostgresTest
    # Read from the gem so a new table can't be missed here — see
    # Repository::ALL_TABLES.
    TABLES = DispatchPolicy::Repository::ALL_TABLES

    # Columns that must exist for the schema to count as current. Add to
    # this when a migration adds a column, per the "Adding a table?"
    # workflow in CLAUDE.md — that's what makes the suite rebuild a
    # stale local database instead of failing on a missing column.
    # Per TABLE. A flat list was checked only against
    # dispatch_policy_partitions, so `failed_at` — which lives on
    # staged_jobs — could never be satisfied and every integration test
    # paid a full DROP CASCADE + re-migrate. Over-detecting drift is the
    # safe direction, but it is ~45% of the suite's wall time and the
    # entry an unwary reader trusts is a no-op.
    SCHEMA_COLUMNS = {
      "dispatch_policy_partitions"  => %w[total_admitted shard decayed_admits
                                          decayed_admits_at scheduled_eligible_at],
      "dispatch_policy_staged_jobs" => %w[failed_at failure_reason]
    }.freeze

    # Every column whose DEFAULT is a clock expression. See defaults_current?.
    TIMESTAMP_DEFAULTS = {
      "dispatch_policy_staged_jobs"   => %w[enqueued_at],
      "dispatch_policy_inflight_jobs" => %w[admitted_at heartbeat_at],
      "dispatch_policy_tick_samples"  => %w[sampled_at]
    }.freeze

    module_function

    # Memoized across the whole run rather than per class: ten classes
    # used to open (and warn about) the same connection independently.
    #
    # Only SUCCESS is memoized. Caching a failure would let one transient
    # hiccup — Postgres restarting, a momentary connection limit — skip
    # every remaining integration test in the run, and (outside CI, where
    # DISPATCH_POLICY_REQUIRE_DB makes it fatal) report green having
    # exercised nothing but the unit tests. Retrying per class costs one
    # failed connect attempt each when the database really is absent.
    def connect!
      return true if @connected

      ActiveRecord::Base.establish_connection(
        adapter:  "postgresql",
        encoding: "unicode",
        host:     ENV.fetch("DB_HOST", "localhost"),
        username: ENV.fetch("DB_USER", ENV["USER"]),
        password: ENV.fetch("DB_PASS", ""),
        database: ENV.fetch("DB_NAME", "dispatch_policy_test")
      )
      ActiveRecord::Base.connection.execute("SELECT 1")
      @connected = true
    rescue StandardError => e
      # Skipping is right for a contributor with no local Postgres, and
      # wrong for CI: a misconfigured service container would skip every
      # integration case and report a green build over an untested gem.
      # DISPATCH_POLICY_REQUIRE_DB turns "no database" into a failure.
      raise if ENV["DISPATCH_POLICY_REQUIRE_DB"] == "1"

      # Warn once per run, not once per test class, while still leaving
      # @connected unset so a later class retries.
      unless @warned
        warn "[skip] Postgres not reachable: #{e.message}"
        @warned = true
      end
      false
    end

    def ensure_schema!
      return if schema_present?

      drop_partial_schema!
      require_relative "../db/migrate/20260501000001_create_dispatch_policy_tables"
      ActiveRecord::Migration.suppress_messages do
        CreateDispatchPolicyTables.new.change
      end
    end

    def schema_present?
      conn = ActiveRecord::Base.connection
      return false unless TABLES.all? { |t| conn.table_exists?(t) }

      # Detect schema drift (e.g. new column added in a migration update),
      # per table — a column checked against the wrong table can never be
      # satisfied, which rebuilds the whole schema before every case.
      return false unless SCHEMA_COLUMNS.all? { |table, required|
        cols = conn.columns(table).map(&:name)
        required.all? { |c| cols.include?(c) }
      }

      defaults_current?(conn)
    end

    # Column DEFAULTS drift too, and the column-presence check above cannot
    # see it: a database created before the defaults changed keeps writing
    # the old expression, and the only symptom is a handful of unrelated
    # assertions failing on that database and nowhere else. That cost an
    # investigation the first time (`now()` -> `(now() AT TIME ZONE 'UTC')`,
    # A13), so the drift check now covers the four timestamp defaults —
    # which are the only defaults in the schema whose VALUE depends on
    # anything.
    def defaults_current?(conn)
      TIMESTAMP_DEFAULTS.all? do |table, columns|
        columns.all? do |name|
          column = conn.columns(table).find { |c| c.name == name }
          # Match on the ZONE, not on the syntax.
          #
          # Postgres deparses this default differently by major version:
          # 13 stores `timezone('UTC'::text, now())`, 16 stores
          # `now() AT TIME ZONE 'utc'::text` — different spelling AND
          # different case. Matching either one exactly is a check that
          # passes on one CI leg and fails on the other, which is precisely
          # what happened twice: the first version matched
          # "AT TIME ZONE" (green on PG16, silently always-rebuilding on
          # PG13), the second matched "timezone('UTC'" (green on PG13, RED
          # on PG16). Both spellings name the zone and a bare `now()` does
          # not, so that is what to look for. The SEMANTIC property — that
          # the default actually stores UTC — belongs to
          # `utc_storage_test`, which exercises it through the real write
          # path under skewed sessions; this only has to be fast and
          # version-proof.
          column && column.default_function.to_s.match?(/utc/i)
        end
      end
    end

    def drop_partial_schema!
      conn = ActiveRecord::Base.connection
      TABLES.each { |t| conn.execute("DROP TABLE IF EXISTS #{t} CASCADE") }
    end

    def truncate_tables!
      ActiveRecord::Base.connection.execute(
        "TRUNCATE #{TABLES.join(', ')} RESTART IDENTITY"
      )
    end
  end
end

class Minitest::Test
  include DispatchPolicy::TestHelpers
  include ActiveSupport::Testing::TimeHelpers

  def setup
    reset_dispatch_policy!
  end
end

module DispatchPolicy
  # Base class for tests that need Postgres. Skips the whole case when no
  # database is reachable, creates the schema if it's missing or stale,
  # and hands every test a clean set of tables. Subclasses add their own
  # policy registration after `super`.
  class IntegrationTest < Minitest::Test
    def setup
      super
      skip "no Postgres available" unless PostgresTest.connect!
      PostgresTest.ensure_schema!
      PostgresTest.truncate_tables!
    end
  end
end
