# frozen_string_literal: true

require "active_job"
require "active_record"

require "dispatch_policy/version"
require "dispatch_policy/engine" if defined?(Rails)

module DispatchPolicy
  Config = Struct.new(
    :enabled,
    :lease_duration,
    :batch_size,
    :tick_max_duration,
    :tick_sleep,
    :tick_sleep_busy,
    :partition_idle_ttl,
    :admin_partition_limit,
    :database_role,
    :allowed_adapters,
    :policy_config_source,
    :auto_tune,
    keyword_init: true
  )

  PG_BACKED_ADAPTERS  = %i[good_job solid_queue].freeze
  IN_PROCESS_ADAPTERS = %i[test inline async].freeze

  def self.config
    @config ||= Config.new(
      enabled:               true,
      lease_duration:        2 * 60,
      batch_size:            500,
      tick_max_duration:     60,
      tick_sleep:            1,
      tick_sleep_busy:       0.05,
      partition_idle_ttl:    30 * 60,
      admin_partition_limit: 5_000,
      database_role:         nil,
      allowed_adapters:      nil,
      policy_config_source:  :db,
      auto_tune:             false
    )
  end

  def self.configure
    yield config
  end

  def self.enabled?
    config.enabled != false
  end

  def self.registry
    @registry ||= {}
  end

  def self.reset_registry!
    @registry = {}
  end
end

require "dispatch_policy/policy"
require "dispatch_policy/dispatchable"
require "dispatch_policy/dispatch"
require "dispatch_policy/tick_loop"
require "dispatch_policy/stats"
require "dispatch_policy/active_job_perform_all_later_patch"
