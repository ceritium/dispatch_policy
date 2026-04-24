# frozen_string_literal: true

require_relative "boot"

require "active_model/railtie"
require "active_job/railtie"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"

Bundler.require(*Rails.groups)
require "dispatch_policy"

module Dummy
  class Application < Rails::Application
    config.root = File.expand_path("..", __dir__)
    config.paths["config/database"] = File.expand_path("database.yml", __dir__)

    config.load_defaults 7.1
    config.eager_load = false
    config.secret_key_base = "test" * 10
    config.active_record.schema_format = :ruby
    config.active_record.migration_error = false
    config.active_job.queue_adapter = :test
    config.paths["db/migrate"] = File.expand_path("../../../db/migrate", __dir__)

    config.generators do |g|
      g.test_framework nil
    end

    config.logger = Logger.new(nil)
  end
end
