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
require_relative "dispatch_policy/overview"
require_relative "dispatch_policy/assets"

module DispatchPolicy
  class Error < StandardError; end
  class PolicyAlreadyRegistered < Error; end
  class UnknownGate < Error; end
  class InvalidPolicy < Error; end
  class EnqueueFailed < Error; end

  # This process cannot resolve the job class. Distinct from any other
  # NameError so the Forwarder holds a row back for the one reason that
  # actually justifies it.
  class UnresolvableJobClass < Error; end

  # A staged row this process cannot deliver. It may become deliverable —
  # a rolling deploy where the tick pod is still on the old image is the
  # ordinary case — so quarantine is a HOLD, retried on a cadence, not a
  # verdict. It carries the staged ids so the caller can hold back exactly
  # those rows and admit the rest, instead of the whole partition wedging
  # behind them.
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
  # The gem's own AR models — Partition, StagedJob, TickSample and the
  # rest — hang off ActiveRecord::Base, so they follow whatever
  # connection the host's models follow. That is right on one database
  # and wrong on the install `database_connection_class` exists for: the
  # gem's tables are on the adapter's database, and `Repository`'s raw
  # SQL goes there, but the models would still look for them on the
  # primary. `Tick#record_sample!` then raises PG::UndefinedTable into
  # its own rescue and tick_samples stay empty forever, and every
  # dashboard page 500s inside the around_action that is supposed to
  # route it.
  #
  # Pinning the connection SPECIFICATION rather than calling
  # `connects_to` keeps this a no-op on a single database and avoids
  # taking over a configuration the host may have set on that class.
  # Note it follows `database_connection_class`, not `database_role`: a
  # deployment needing both should point the models at the same
  # configuration itself.
  def route_models_to_configured_connection!
    base = Repository.base_class
    return if base == ActiveRecord::Base
    return unless defined?(DispatchPolicy::ApplicationRecord)

    DispatchPolicy::ApplicationRecord.connection_specification_name =
      base.connection_specification_name
  rescue StandardError => e
    config.logger&.warn(
      "[dispatch_policy] could not route the engine's models to " \
      "#{config.database_connection_class.inspect}: #{e.class}: #{e.message}"
    )
  end

  def warn_unsupported_adapter
    return unless defined?(::ActiveJob::Base)
    adapter = ::ActiveJob::Base.queue_adapter
    return unless adapter

    warn_unsupported_adapter_for(adapter)
  end

  # Split out so it is reachable from a test without swapping the
  # process's queue adapter — these two warnings are the only thing
  # standing between a split connection and a silent at-least-once loss,
  # and they had no coverage at all.
  def warn_unsupported_adapter_for(adapter)

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
    adapter_class = adapter_record_class(adapter)
    return unless adapter_class.respond_to?(:connection_specification_name)

    ours = Repository.base_class
    return if adapter_class == ours ||
              adapter_class.connection_specification_name == ours.connection_specification_name

    config.logger&.warn(
      "[dispatch_policy] the adapter writes through #{adapter_class.name} but the gem opens its " \
      "transaction on #{ours.name}: the adapter's INSERT will NOT join the admission transaction, " \
      "so a crash between COMMIT and enqueue loses the job. " \
      "Set config.database_connection_class = #{adapter_class.name.inspect}."
    )
  end

  # The explicit check first, and `inherit: false` on the lookup: a plain
  # `const_defined?(:Record)` walks up to Object, so an ordinary host
  # model named `Record` is found instead. That is not cosmetic — an AR
  # `::Record` on the primary makes the specification names match and
  # silences this warning on exactly the misconfiguration it exists to
  # catch, and a non-AR `::Record` (a Struct, a PORO) makes
  # `connection_specification_name` raise out of after_initialize and
  # abort boot.
  def adapter_record_class(adapter)
    name = adapter.class.name.to_s
    return ::SolidQueue::Record if name.include?("SolidQueue") && defined?(::SolidQueue::Record)
    # good_job writes through GoodJob::BaseRecord and defines no :Record
    # anywhere on its adapter, so without this the warning could never
    # fire for it — on the adapter this project defaults to, and whose
    # hint also suppresses the generic one, leaving boot completely
    # silent on a split that loses jobs.
    return ::GoodJob::BaseRecord if name.include?("GoodJob") && defined?(::GoodJob::BaseRecord)

    klass = adapter.class
    klass.const_get(:Record, false) if klass.const_defined?(:Record, false)
  end
end

require_relative "dispatch_policy/railtie" if defined?(Rails::Railtie)
require_relative "dispatch_policy/engine"  if defined?(Rails::Engine)
