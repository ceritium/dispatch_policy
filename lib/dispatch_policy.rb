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
require_relative "dispatch_policy/cursor_pagination"
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

  # Adapters whose enqueue runs against ActiveRecord::Base.connection (so
  # the adapter INSERT can join the admission TX) or whose semantics make
  # atomicity moot (test/inline). Substring match against the adapter
  # class name keeps the check resilient to ActiveJob's wrapper renames.
  PG_BACKED_ADAPTER_HINTS = %w[GoodJob SolidQueue].freeze
  EXEMPT_ADAPTER_HINTS    = %w[Test Inline Async].freeze

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

  # Logs a warning if the configured ActiveJob adapter is not one of the
  # PG-backed ones the gem can guarantee atomic admission for. We do NOT
  # raise: a host may use a custom PG-backed adapter we don't recognize,
  # or may have accepted the trade-off knowingly. The warning is enough
  # to surface the issue at boot.
  def warn_unsupported_adapter
    return unless defined?(::ActiveJob::Base)
    adapter = ::ActiveJob::Base.queue_adapter
    return unless adapter

    klass_name = adapter.class.name.to_s
    return if (PG_BACKED_ADAPTER_HINTS + EXEMPT_ADAPTER_HINTS).any? { |hint| klass_name.include?(hint) }

    config.logger&.warn(
      "[dispatch_policy] active_job adapter is #{klass_name}; atomic admission requires " \
      "a PG-backed adapter that shares ActiveRecord::Base's connection (good_job, solid_queue). " \
      "If the worker process crashes between admission COMMIT and adapter enqueue, the job is lost. " \
      "Set DispatchPolicy.config.database_role if you use a separate DB role for queueing."
    )
  end
end

require_relative "dispatch_policy/railtie" if defined?(Rails::Railtie)
require_relative "dispatch_policy/engine"  if defined?(Rails::Engine)
