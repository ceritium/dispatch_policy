# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
  class Engine < ::Rails::Engine
    isolate_namespace DispatchPolicy

    initializer "dispatch_policy.assets" do |app|
      if app.config.respond_to?(:assets) && app.config.assets.respond_to?(:precompile)
        app.config.assets.precompile += %w[dispatch_policy/application.css]
      end
    end

    initializer "dispatch_policy.action_dispatch" do
      Rails.application.config.assets.paths << root.join("app/assets/stylesheets") if Rails.application.config.respond_to?(:assets)
    rescue StandardError
      # propshaft / no asset pipeline at all — ignore
    end
  end
end
