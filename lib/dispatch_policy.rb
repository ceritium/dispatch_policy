# frozen_string_literal: true

require "active_support"
require "active_support/core_ext"
require "active_job"

require_relative "dispatch_policy/version"
require_relative "dispatch_policy/config"
require_relative "dispatch_policy/context"
require_relative "dispatch_policy/policy"
require_relative "dispatch_policy/registry"
require_relative "dispatch_policy/serializer"
require_relative "dispatch_policy/bypass"
require_relative "dispatch_policy/decision"
require_relative "dispatch_policy/gate"
require_relative "dispatch_policy/gates/throttle"
require_relative "dispatch_policy/gates/concurrency"
require_relative "dispatch_policy/policy_dsl"
require_relative "dispatch_policy/pipeline"
require_relative "dispatch_policy/repository"
require_relative "dispatch_policy/forwarder"
require_relative "dispatch_policy/inflight_tracker"
require_relative "dispatch_policy/tick"
require_relative "dispatch_policy/tick_loop"
require_relative "dispatch_policy/job_extension"

module DispatchPolicy
  class Error < StandardError; end
  class PolicyAlreadyRegistered < Error; end
  class UnknownGate < Error; end
  class InvalidPolicy < Error; end

  module_function

  def configure
    yield config
  end

  def config
    @config ||= Config.new
  end

  def reset_config!
    @config = Config.new
  end

  def registry
    @registry ||= Registry.new
  end

  def reset_registry!
    @registry = Registry.new
  end
end

require_relative "dispatch_policy/railtie" if defined?(Rails::Railtie)
require_relative "dispatch_policy/engine"  if defined?(Rails::Engine)
