# frozen_string_literal: true

module DispatchPolicy
  class Config
    attr_accessor :enabled,
                  :tick_max_duration,
                  :partition_batch_size,
                  :admission_batch_size,
                  :idle_pause,
                  :busy_pause,
                  :partition_inactive_after,
                  :quarantine_retry_after,
                  :unknown_policy_retention,
                  :inflight_stale_after,
                  :inflight_queued_stale_after,
                  :inflight_heartbeat_interval,
                  :real_adapter,
                  :logger,
                  :clock,
                  :sweep_every_ticks,
                  :metrics_retention,
                  :database_role,
                  :database_connection_class,
                  :fairness_half_life_seconds,
                  :tick_admission_budget,
                  :adapter_throughput_target,
                  :forward_failure_backoff

    def initialize
      # Master switch for the ENQUEUE side. When false, the around_enqueue
      # and the BulkEnqueue patch pass through to the real adapter without
      # staging, so the gem becomes a no-op for new perform_later calls.
      # The TickLoop keeps running: whatever is already staged still needs
      # admitting, and with staging off nothing else will ever hand those
      # rows to the adapter. That is what makes this usable for a cutover
      # — flip it, watch the backlog drain, then stop the tick job. To
      # stop admission itself, stop the tick job or pause the policy from
      # the dashboard (claim_partitions honors the pause flag).
      @enabled                   = true
      @tick_max_duration         = 25
      @partition_batch_size      = 50
      @admission_batch_size      = 100
      @idle_pause                = 0.5
      # Sleep between iterations when the previous tick admitted > 0
      # jobs. 0 (default) preserves the original "busy = no pause"
      # behavior. Set to a small value (e.g. 0.02) to back off the DB
      # when several TickLoops compete for connections; the per-loop
      # throughput ceiling becomes admission_batch_size / busy_pause.
      @busy_pause                = 0.0
      @partition_inactive_after  = 24 * 60 * 60
      # How long a staged row the Forwarder could not deliver is held back
      # before the sweeper tries it again. The trigger is "this process
      # cannot resolve the job class", which a rolling deploy produces and
      # then resolves minutes later — so the hold has to expire, or an
      # ordinary deploy drops that class's whole backlog silently and for
      # good. A class that really is gone just re-quarantines, at a couple
      # of rows per tick per hour. Set to 0 to hold forever.
      @quarantine_retry_after    = 60 * 60
      # How long a partition whose policy is absent from THIS process's
      # registry is kept when it still carries a token bucket. "Absent
      # from the registry" is not the same as "deleted from the code" —
      # the registry is populated as a side effect of job classes
      # loading, so lazy loading, a dashboard-only process or a rolling
      # deploy all produce it (see ISSUES.md R3, the same mistake in
      # ManualAdmission). Collecting such a row resets its throttle
      # bucket and hands that tenant a fresh quota, so it waits out a
      # grace long enough to cover any plausible window instead of
      # partition_inactive_after. Rows with no bucket are collected on
      # the normal cutoff — there is nothing to lose.
      @unknown_policy_retention  = 30 * 24 * 60 * 60
      @inflight_stale_after      = 5 * 60
      # Cutoff for inflight rows that were admitted (pre-inserted by the
      # Tick) but never started performing — so the heartbeat thread, which
      # only starts in around_perform, never advanced their heartbeat_at.
      # These sit in the adapter's queue waiting for a worker; reaping them
      # at `inflight_stale_after` (5 min) would make the concurrency gate
      # under-count and over-admit whenever queue latency exceeds that. We
      # give never-started rows a far more generous cutoff (1h) before
      # assuming the admission was lost. Raise it if your adapter backlog
      # can exceed an hour.
      @inflight_queued_stale_after = 60 * 60
      # Seconds between heartbeat_at refreshes. Each beat checks out a
      # connection from the role's pool for the duration of one UPDATE and
      # returns it explicitly — the heartbeat thread runs outside the
      # Rails executor, where the pool treats a lease as permanent and
      # `with_connection` alone would NOT give it back (see
      # InflightTracker.beat!). A little pool headroom above the worker
      # concurrency is still worth having, since a beat and its job can
      # want a connection at the same instant. Set to 0 to disable the
      # heartbeat thread entirely.
      @inflight_heartbeat_interval = 30
      @real_adapter              = nil
      @logger                    = nil
      @clock                     = -> { Time.now.utc }
      @sweep_every_ticks         = 50
      @metrics_retention         = 24 * 60 * 60
      # AR role for the admission TX. nil = default connection. Set to
      # e.g. :queue when the host runs solid_queue on a separate DB.
      @database_role             = nil
      # The ActiveRecord class the gem opens its connection on. nil means
      # ActiveRecord::Base, which is right unless the adapter writes
      # through a different one: on a separate-queue-database install set
      # it to the adapter's record class ("SolidQueue::Record", or
      # good_job's active_record_parent_class). The gem's guarantee is
      # that the adapter's INSERT joins the admission transaction, and
      # that only holds when both are on the same connection.
      @database_connection_class = nil
      # Fairness: the half-life of decayed_admits (per-partition EWMA).
      # 60s means a partition's "recent activity" weight halves every
      # 60s of idleness. Tick reorders claimed partitions by lowest
      # decayed_admits first; under-admitted ones get first crack.
      @fairness_half_life_seconds = 60
      # Optional global cap on admissions per tick. nil = no cap; each
      # partition uses admission_batch_size as its ceiling. When set,
      # fair_share = ceil(cap / partitions_seen) is the per-partition
      # ceiling, with redistribution of leftover budget after pass-1.
      @tick_admission_budget     = nil
      # Operator-supplied "ceiling" of the underlying adapter, in jobs
      # per second. The dashboard renders the live admit rate as a
      # percentage of this and fires a hint when we're closing on it.
      # nil = no ceiling reference (just shows the absolute rate).
      # Measured locally against good_job: ~3500 jobs/sec per worker.
      @adapter_throughput_target = nil
      # How long a partition backs off after its admission raised — an
      # adapter refusing enqueues, or a gate with a bug. Without it the
      # tick re-claims and re-fails that partition every iteration, which
      # burns a claim slot and a transaction per tick and buries the logs
      # in the same error. Keep it short: it also delays recovery once
      # whatever broke is fixed. 0 disables the backoff.
      @forward_failure_backoff   = 5
    end

    def now
      @clock.call
    end

    def logger
      @logger || (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) || default_logger
    end

    private

    def default_logger
      require "logger"
      @default_logger ||= Logger.new($stdout, level: Logger::INFO)
    end
  end
end
