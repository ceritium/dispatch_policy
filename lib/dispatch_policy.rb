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
    :partition_drained_purge_threshold,
    :partition_drained_purge_min_total,
    :admin_partition_limit,
    :database_role,
    :allowed_adapters,
    :policy_config_source,
    :auto_tune,
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
      # considers per tick. nil falls back to batch_size — the smallest
      # value that lets the LATERAL fully fill the batch (cap × quantum
      # ≥ batch_size when quantum ≥ 1) without falling back to the
      # top-up path. Override only if you want a tighter ceiling on
      # fetch wall time at the cost of slower rotation.
      round_robin_max_partitions_per_tick: nil,
      tick_max_duration:     60,               # 1.minute
      tick_sleep:            1,                # idle sleep
      tick_sleep_busy:       0.05,             # busy sleep
      partition_idle_ttl:    30 * 60,          # 30.minutes
      # Aggressive purge of drained partition_states rows. When the
      # ratio of drained (pending_count = 0) to total rows exceeds
      # this threshold AND the table has at least
      # partition_drained_purge_min_total rows, all drained rows are
      # deleted regardless of last_admitted_at age. Insurance for
      # policies that churn through many short-lived partitions.
      # Set to nil to disable.
      partition_drained_purge_threshold: 0.5,
      partition_drained_purge_min_total: 10_000,
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
      allowed_adapters:      nil,
      # Source of truth for per-policy config overrides on TickLoop
      # boot.
      #   :db   — DB rows win. The Policy DSL still seeds rows on first
      #           boot, but afterwards the live values in
      #           dispatch_policy_policy_configs override the code DSL.
      #           Use this when you want to tune at runtime via the
      #           console / admin UI without redeploying.
      #   :code — Code DSL wins. Every TickLoop boot mirrors the DSL
      #           values back into the DB (source: "code"), erasing
      #           any UI-driven changes. Use this when the DSL block
      #           is the canonical source of truth.
      policy_config_source:  :db,
      # Closed-loop self-tuning. When :apply, the TickLoop calls
      # Stats.bottleneck for each round-robin policy at boot and
      # persists the recommended_config knobs (batch_size,
      # round_robin_quantum) to the DB-backed table with
      # source: "auto". The next reload picks them up. Set to
      # :recommend to log recommendations without writing, or false
      # to disable.
      auto_tune:             false
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
require "dispatch_policy/stats"
require "dispatch_policy/active_job_perform_all_later_patch"
