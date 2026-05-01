# frozen_string_literal: true

require "rails/railtie"

module DispatchPolicy
  class Railtie < ::Rails::Railtie
    initializer "dispatch_policy.active_job" do
      ActiveSupport.on_load(:active_job) do
        include DispatchPolicy::JobExtension
      end

      ActiveSupport.on_load(:active_job) do
        if defined?(ActiveJob) && ActiveJob.respond_to?(:perform_all_later)
          singleton = ActiveJob.singleton_class
          unless singleton.include?(DispatchPolicy::JobExtension::BulkEnqueue)
            singleton.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
          end
        end
      end
    end

    initializer "dispatch_policy.migration_paths" do |app|
      gem_root = File.expand_path("../..", __dir__)
      app.config.paths["db/migrate"] << File.join(gem_root, "db/migrate") if File.directory?(File.join(gem_root, "db/migrate"))
    end
  end
end
