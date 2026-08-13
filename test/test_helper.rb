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
    TABLES = %w[
      dispatch_policy_staged_jobs
      dispatch_policy_partitions
      dispatch_policy_inflight_jobs
      dispatch_policy_tick_samples
      dispatch_policy_adaptive_concurrency_stats
      dispatch_policy_policy_settings
    ].freeze

    # Columns that must exist for the schema to count as current. Add to
    # this when a migration adds a column, per the "Adding a table?"
    # workflow in CLAUDE.md — that's what makes the suite rebuild a
    # stale local database instead of failing on a missing column.
    SCHEMA_COLUMNS = %w[total_admitted shard decayed_admits decayed_admits_at].freeze

    module_function

    # Memoized across the whole run rather than per class: ten classes
    # used to open (and warn about) the same connection independently.
    def connect!
      return @connected unless @connected.nil?

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
      warn "[skip] Postgres not reachable: #{e.message}"
      @connected = false
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

      # Detect schema drift (e.g. new column added in a migration update).
      cols = conn.columns("dispatch_policy_partitions").map(&:name)
      SCHEMA_COLUMNS.all? { |c| cols.include?(c) }
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
