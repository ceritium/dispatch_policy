# frozen_string_literal: true

require "rails/generators"
require "rails/generators/migration"

module DispatchPolicy
  module Generators
    class InstallGenerator < ::Rails::Generators::Base
      include ::Rails::Generators::Migration

      source_root File.expand_path("templates", __dir__)
      desc "Installs dispatch_policy: migration, initializer, and tick loop job."

      def self.next_migration_number(_path)
        Time.now.utc.strftime("%Y%m%d%H%M%S")
      end

      def copy_migration
        migration_template "create_dispatch_policy_tables.rb.tt",
                           "db/migrate/create_dispatch_policy_tables.rb"
      end

      def create_initializer
        template "initializer.rb.tt", "config/initializers/dispatch_policy.rb"
      end

      def create_tick_loop_job
        template "dispatch_tick_loop_job.rb.tt", "app/jobs/dispatch_tick_loop_job.rb"
      end

      def show_readme
        readme_text = <<~MSG

          dispatch_policy installed.

          Next steps:
            1) bin/rails db:migrate
            2) Mount the engine in config/routes.rb:
                 mount DispatchPolicy::Engine, at: "/dispatch_policy"
            3) Schedule DispatchTickLoopJob (cron / good_job recurring / solid_queue recurring)
               and start it once: DispatchTickLoopJob.perform_later
            4) Declare a policy in any ActiveJob:
                 dispatch_policy :name do
                   context ->(args) { { ... } }
                   partition_by ->(c) { c[:key] }
                   gate :throttle, rate: 10, per: 60
                 end

        MSG
        say readme_text, :green
      end

      private

      def good_job?
        adapter_name == "good_job"
      end

      def solid_queue?
        adapter_name == "solid_queue"
      end

      def adapter_name
        Rails.application.config.active_job.queue_adapter.to_s
      rescue StandardError
        nil
      end
    end
  end
end
