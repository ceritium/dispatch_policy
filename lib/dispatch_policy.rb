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
    :round_robin_quantum,
    :round_robin_max_partitions_per_tick,
    :tick_max_duration,
    :tick_sleep,
    :tick_sleep_busy,
    :partition_idle_ttl,
    :admin_partition_limit,
    :database_role,
    :allowed_adapters,
    keyword_init: true
  )

  # Adapters that store jobs in the same PostgreSQL database as the host
  # app. These can participate in the staging transaction so admission +
  # enqueue is atomic. Sidekiq/Resque/SQS are not supported because their
  # backing store is external — see docs/architecture.md.
  PG_BACKED_ADAPTERS = %i[good_job solid_queue].freeze

  # Adapters that don't talk to any external store, safe to use in tests
  # and development. Atomicity is trivially preserved.
  IN_PROCESS_ADAPTERS = %i[test inline async].freeze

  def self.config
    @config ||= Config.new(
      enabled:               true,
      # Reaper safety net for the narrow case "worker started the job
      # but died before around_perform.ensure released counters".
      # Admission/enqueue is atomic so this never covers stuck-admitted
      # rows anymore — keep it short.
      lease_duration:        2 * 60,           # 2.minutes
      batch_size:            500,
      round_robin_quantum:   50,
      # Cap on how many partitions a round-robin / time-weighted fetch
      # considers per tick. nil means "all active partitions". When
      # set, partitions are picked least-recently-admitted first
      # (rotating LRU cursor in dispatch_policy_partition_states),
      # bounding fetch wall time at high cardinality. Round-trip
      # guarantee: every active partition entered the batch within
      # ceil(active_partitions / cap) ticks.
      round_robin_max_partitions_per_tick: nil,
      tick_max_duration:     60,               # 1.minute
      tick_sleep:            1,                # idle sleep
      tick_sleep_busy:       0.05,             # busy sleep
      partition_idle_ttl:    30 * 60,          # 30.minutes
      # Hard cap on rows the admin's partition breakdown will pull per
      # aggregation. Protects the host DB and process when a policy has
      # tens of thousands of partitions: the admin shows the top-N most
      # active and a truncation banner instead of dragging in everything.
      admin_partition_limit: 5_000,
      # Role used by ApplicationRecord#connects_to when the host app
      # stores jobs in a separate database (typical Solid Queue setup).
      # nil = use the primary connection.
      database_role:         nil,
      # Override the adapter allowlist if you know what you're doing.
      # Defaults to PG_BACKED_ADAPTERS + IN_PROCESS_ADAPTERS.
      allowed_adapters:      nil
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
