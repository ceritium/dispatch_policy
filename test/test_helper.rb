# frozen_string_literal: true

ENV["RAILS_ENV"] = "test"

require "simplecov"
SimpleCov.start do
  add_filter "/test/"
  add_filter "/db/migrate/"
  add_filter "/lib/dispatch_policy/version.rb"
  add_filter "/lib/dispatch_policy/install_generator.rb"
  enable_coverage :branch
  track_files "{lib,app}/**/*.rb"
end

require_relative "dummy/config/environment"

# Run migrations before rails/test_help triggers the pending-migration check.
ActiveRecord::Migration.verbose = false
migration_path = File.expand_path("../db/migrate", __dir__)
ActiveRecord::Base.connection.drop_table :dispatch_policy_staged_jobs, if_exists: true
ActiveRecord::Base.connection.drop_table :dispatch_policy_partition_counts, if_exists: true
ActiveRecord::Base.connection.drop_table :dispatch_policy_throttle_buckets, if_exists: true
ActiveRecord::Base.connection.drop_table :schema_migrations, if_exists: true
ActiveRecord::Base.connection.drop_table :ar_internal_metadata, if_exists: true
ActiveRecord::MigrationContext.new(migration_path).migrate

require "rails/test_help"
require "minitest/autorun"

class ActiveSupport::TestCase
  self.use_transactional_tests = true
end
