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
require_relative "dispatch_policy/gates/adaptive_concurrency"
require_relative "dispatch_policy/policy_dsl"
require_relative "dispatch_policy/cursor_pagination"
require_relative "dispatch_policy/pipeline"
require_relative "dispatch_policy/repository"
require_relative "dispatch_policy/forwarder"
require_relative "dispatch_policy/manual_admission"
require_relative "dispatch_policy/inflight_tracker"
require_relative "dispatch_policy/tick"
require_relative "dispatch_policy/tick_loop"
require_relative "dispatch_policy/job_extension"
require_relative "dispatch_policy/operator_hints"
require_relative "dispatch_policy/assets"

module DispatchPolicy
  class Error < StandardError; end
  class PolicyAlreadyRegistered < Error; end
  class UnknownGate < Error; end
  class InvalidPolicy < Error; end
  class EnqueueFailed < Error; end

  # A staged row that can never be delivered, however many times it is
  # retried — today, a job_class this process cannot resolve. Carries the
  # staged ids so the caller can quarantine exactly those rows and admit
  # the rest, instead of the whole partition wedging behind them.
  class UndeliverableJob < Error
    attr_reader :staged_ids

    def initialize(message, staged_ids:)
      super(message)
      @staged_ids = staged_ids
    end
  end

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
    if (PG_BACKED_ADAPTER_HINTS + EXEMPT_ADAPTER_HINTS).any? { |hint| klass_name.include?(hint) }
      return warn_split_connection(adapter)
    end

    config.logger&.warn(
      "[dispatch_policy] active_job adapter is #{klass_name}; atomic admission requires " \
      "a PG-backed adapter that shares the gem's connection (good_job, solid_queue). " \
      "If the worker process crashes between admission COMMIT and adapter enqueue, the job is lost. " \
      "See config.database_connection_class if the adapter writes through its own record class."
    )
  end

  # The guarantee is that the adapter's INSERT joins the admission
  # transaction, and that only holds while both are on ONE connection. A
  # PG-backed adapter writing through its own record class — which is
  # exactly the separate-queue-database install the README documents — is
  # therefore only safe once `config.database_connection_class` names
  # that class. Say so at boot rather than letting it look like it works.
  def warn_split_connection(adapter)
    adapter_class = adapter.class.const_defined?(:Record) ? adapter.class.const_get(:Record) : nil
    adapter_class ||= defined?(::SolidQueue::Record) && klass_name_matches?(adapter, "SolidQueue") ? ::SolidQueue::Record : nil
    return if adapter_class.nil?

    ours = Repository.base_class
    return if adapter_class == ours || adapter_class.connection_specification_name == ours.connection_specification_name

    config.logger&.warn(
      "[dispatch_policy] the adapter writes through #{adapter_class.name} but the gem opens its " \
      "transaction on #{ours.name}: the adapter's INSERT will NOT join the admission transaction, " \
      "so a crash between COMMIT and enqueue loses the job. " \
      "Set config.database_connection_class = #{adapter_class.name.inspect}."
    )
  end

  def klass_name_matches?(adapter, hint)
    adapter.class.name.to_s.include?(hint)
  end
end

require_relative "dispatch_policy/railtie" if defined?(Rails::Railtie)
require_relative "dispatch_policy/engine"  if defined?(Rails::Engine)
