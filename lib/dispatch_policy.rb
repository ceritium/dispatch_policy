# frozen_string_literal: true

require "active_job"
require "active_record"

require "dispatch_policy/version"
require "dispatch_policy/engine" if defined?(Rails)

module DispatchPolicy
  # Hard cap on the length of partition keys (gate partition_by results,
  # round_robin_by results). Keys longer than this are truncated before
  # they reach the database. Protects partition_counts / throttle_buckets
  # / partition_observations rows + indexes from being inflated by an
  # accidentally-unbounded host-app input flowing through a partition_by
  # lambda.
  MAX_PARTITION_KEY_LENGTH = 512

  Config = Struct.new(
    :enabled,
    :lease_duration,
    :batch_size,
    :round_robin_quantum,
    :tick_max_duration,
    :tick_sleep,
    :tick_sleep_busy,
    :partition_idle_ttl,
    :admin_partition_limit,
    keyword_init: true
  )

  def self.config
    @config ||= Config.new(
      enabled:               true,
      lease_duration:        15 * 60,          # 15.minutes
      batch_size:            500,
      round_robin_quantum:   50,
      tick_max_duration:     60,               # 1.minute
      tick_sleep:            1,                # idle sleep
      tick_sleep_busy:       0.05,             # busy sleep
      partition_idle_ttl:    30 * 60,          # 30.minutes
      # Hard cap on rows the admin's partition breakdown will pull per
      # aggregation. Protects the host DB and process when a policy has
      # tens of thousands of partitions: the admin shows the top-N most
      # active and a truncation banner instead of dragging in everything.
      admin_partition_limit: 5_000
    )
  end

  def self.configure
    yield config
  end

  def self.enabled?
    config.enabled != false
  end

  # Registry: policy_name => job_class. Populated by Policy#initialize.
  def self.registry
    @registry ||= {}
  end

  def self.reset_registry!
    @registry = {}
  end
end

require "dispatch_policy/policy"
require "dispatch_policy/gate"
require "dispatch_policy/gates/concurrency"
require "dispatch_policy/gates/throttle"
require "dispatch_policy/gates/global_cap"
require "dispatch_policy/gates/fair_interleave"
require "dispatch_policy/gates/adaptive_concurrency"
require "dispatch_policy/dispatch_context"
require "dispatch_policy/dispatchable"
require "dispatch_policy/tick"
require "dispatch_policy/tick_loop"
require "dispatch_policy/active_job_perform_all_later_patch"
