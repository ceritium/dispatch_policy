# frozen_string_literal: true

require_relative "boot"

require "rails"
require "active_model/railtie"
require "active_job/railtie"
require "active_record/railtie"
require "action_controller/railtie"
require "action_view/railtie"

Bundler.require(*Rails.groups)

require "dispatch_policy"

ADAPTER = (ENV["DUMMY_ADAPTER"] || "good_job").to_s
case ADAPTER
when "good_job"
  require "good_job"
when "solid_queue"
  require "solid_queue"
else
  raise "Unknown DUMMY_ADAPTER: #{ADAPTER}. Use good_job or solid_queue."
end

module DummyApp
  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.to_f
    config.eager_load = false
    config.api_only   = false
    config.consider_all_requests_local = true
    config.hosts.clear if config.respond_to?(:hosts)

    config.active_job.queue_adapter = ADAPTER.to_sym

    if config.respond_to?(:assets)
      config.assets.compile  = true
      config.assets.debug    = false
    end

    config.session_store :cookie_store, key: "_dispatch_policy_dummy_session"
    config.middleware.use ActionDispatch::Cookies
    config.middleware.use config.session_store, config.session_options
  end
end
