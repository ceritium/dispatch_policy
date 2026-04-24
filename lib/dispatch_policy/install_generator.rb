# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"

module DispatchPolicy
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include Rails::Generators::Migration

      source_root File.expand_path("../../db/migrate", __dir__)

      def self.next_migration_number(dirname)
        ActiveRecord::Generators::Base.next_migration_number(dirname)
      end

      def copy_migration
        migration_template "20260424000001_create_dispatch_policy_tables.rb",
                           "db/migrate/create_dispatch_policy_tables.rb"
      end
    end
  end
end
