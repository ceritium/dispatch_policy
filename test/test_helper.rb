# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"

unless ENV["SIMPLECOV_DISABLED"] == "1"
  require "simplecov"
  SimpleCov.start do
    add_filter "/test/"
    add_filter "/db/migrate/"
    add_filter "/lib/dispatch_policy/version.rb"
    enable_coverage :branch
    track_files "{lib,app}/**/*.rb"
  end
end

require_relative "dummy/config/environment"

# Run migrations before rails/test_help triggers the pending-migration check.
ActiveRecord::Migration.verbose = false
migration_path = File.expand_path("../db/migrate", __dir__)
%w[
  dispatch_policy_staged_jobs
  dispatch_policy_partitions
  dispatch_policy_policy_configs
  dispatch_policy_tick_runs
  schema_migrations
  ar_internal_metadata
].each { |t| ActiveRecord::Base.connection.drop_table t.to_sym, if_exists: true }
ActiveRecord::MigrationContext.new(migration_path).migrate

require "rails/test_help"
require "minitest/autorun"

class ActiveSupport::TestCase
  self.use_transactional_tests = true
end
