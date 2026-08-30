# frozen_string_literal: true

module DispatchPolicy
  # Around-perform that records each job execution in
  # dispatch_policy_inflight_jobs while it runs, so the concurrency gate
  # can count active jobs per partition.
  #
  # While the job runs we spawn a heartbeat thread that bumps
  # `heartbeat_at` every `config.inflight_heartbeat_interval` seconds.
  # Without this, jobs longer than `inflight_stale_after` (default 5 min)
  # get their inflight row prematurely swept and the concurrency gate
  # over-admits.
  module InflightTracker
    extend ActiveSupport::Concern

    # Gate types whose admission decision is a COUNT(*) over
    # dispatch_policy_inflight_jobs. A policy declaring one of these needs
    # BOTH ends of the row lifecycle — the Tick pre-inserts on admission,
    # the around_perform below releases on completion. A policy with none
    # of them needs neither. See Policy#inflight_tracked_gate.
    TRACKED_GATES = %i[concurrency adaptive_concurrency].freeze

    included do
      # Opt-in for a policy WITHOUT a tracked gate: "count my jobs in the
      # dashboard anyway". Not a marker of whether the callback below was
      # installed — including this module IS the installation, so the two
      # can't disagree.
      class_attribute :dispatch_policy_force_inflight_tracking,
                      instance_writer: false, default: false

      # The callback is installed by the mere act of including this
      # module, and `track` decides per job whether to do anything. That
      # is the whole point: creation (Tick/ManualAdmission, from the
      # registered policy) and release (here) must never be able to
      # disagree about which jobs are tracked. When installation was a
      # separate step keyed on the `dispatch_policy` macro, a class wired
      # through `dispatch_policy_name=` got rows created and never
      # released, and its partition wedged at `max` for an hour at a time.
      #
      # ActiveSupport::Concern's append_features returns early when the
      # target already has this module as an ancestor, so the railtie's
      # include into ActiveJob::Base and a job class's own include add
      # exactly one callback between them — nesting two `track` wrappers
      # would record two adaptive observations per perform and let the
      # inner `ensure` delete the row while the outer one still runs.
      if respond_to?(:around_perform)
        around_perform do |job, block|
          DispatchPolicy::InflightTracker.track(job, &block)
        end
      end
    end

    class_methods do
      # Track this class's jobs even when its policy declares no
      # concurrency-family gate — the way to get a live in-flight count on
      # the dashboard for, say, a throttle-only policy. Policies WITH such
      # a gate are tracked without this: they're the ones the count exists
      # for, so `track` reads the policy rather than trusting a class-level
      # declaration to be remembered.
      def dispatch_policy_inflight_tracking
        self.dispatch_policy_force_inflight_tracking = true
      end
    end

    # Creation half of the row lifecycle, called from inside the admission
    # transaction by Tick and ManualAdmission. It lives next to the release
    # half deliberately: the two must agree on when a row exists, and they
    # drifted once already (audit H3).
    #
    # `partition_key` is the partition row's own key, which is also what
    # the concurrency gates count against — they read it off the same row
    # rather than recomputing `policy.partition_for(ctx)`. The two agree
    # until somebody edits `partition_by`, and then a recomputed key
    # cannot see the rows written here, so the cap lapses for every
    # partition that predates the edit. Recomputing per row also cost a
    # deep context copy, a mutex-guarded registry lookup and a user proc
    # call inside the admission transaction, to arrive back at the value
    # the caller already holds.
    def self.pre_insert_admitted!(policy_name:, policy:, partition_key:, rows:)
      # Skip only when we KNOW the policy has no gate that reads these
      # rows. An unregistered policy — a web process whose registry never
      # loaded the job class — is not evidence of that, and guessing
      # "no rows" there under-counts the gate and over-admits. A row too
      # many is reclaimed by the sweeper; a row too few is a correctness
      # bug.
      return if policy && policy.inflight_tracked_gate.nil?

      inflight = rows.filter_map do |row|
        ajid = row.dig("job_data", "job_id")
        next unless ajid

        { policy_name: policy_name, partition_key: partition_key, active_job_id: ajid }
      end
      Repository.insert_inflight!(inflight) if inflight.any?
    end

    def self.track(job)
      policy_name = job.class.respond_to?(:dispatch_policy_name) && job.class.dispatch_policy_name
      return yield unless policy_name

      policy = DispatchPolicy.registry.fetch(policy_name)
      # The job names a policy this process can't see — renamed or removed
      # while a tick still running the old code admitted it. That tick may
      # have pre-inserted a row nothing else will ever delete, so release
      # it: the DELETE keys on active_job_id alone and needs neither the
      # policy nor its context. Without this the row holds a concurrency
      # slot until the queued sweeper reclaims it an hour later.
      return release_after(job) { yield } unless policy

      return yield unless tracking?(policy, job.class)

      # Mirror the stage-time fallback in JobExtension.around_enqueue_for:
      # when the job carries no explicit queue, use the policy's default.
      # Without this, a policy whose partition_by/shard_by reads queue_name
      # would compute a DIFFERENT partition_key here than at admission, so
      # the around_perform inflight row (and adaptive observations) would
      # land under the wrong scope and the concurrency gate's COUNT(*) would
      # miss them.
      queue_name    = job.queue_name&.to_s || policy.queue_name
      ctx           = policy.build_context(job.arguments, queue_name: queue_name)
      partition_key = policy.partition_key_for(ctx)

      adaptive_gates = policy.gates.select { |g| g.name == :adaptive_concurrency }
      admitted_at     = nil
      observation_key = nil
      perform_start   = nil
      heartbeat      = nil
      started        = false
      succeeded      = false

      # insert + heartbeat spawn live INSIDE the begin so the ensure always
      # cleans up: if start_heartbeat (Thread.new) raises after the row is
      # inserted, the row would otherwise orphan until the stale sweeper.
      begin
        Repository.insert_inflight!([{
          policy_name:    policy.name,
          partition_key:  partition_key,
          active_job_id:  job.job_id
        }])

        if adaptive_gates.any?
          admitted_at, admitted_key = lookup_admission(job.job_id)
          # Fall back to the recomputed key only when the row is gone.
          observation_key = admitted_key || partition_key
        end
        perform_start = Time.current
        heartbeat     = start_heartbeat(job.job_id)

        started = true
        yield
        succeeded = true
      ensure
        stop_heartbeat(heartbeat)

        # Only record an observation if we actually reached perform — a
        # failure in setup (insert / heartbeat spawn) isn't a perform result.
        if started
          record_adaptive_observations(
            policy:        policy,
            gates:         adaptive_gates,
            partition_key: observation_key || partition_key,
            admitted_at:   admitted_at,
            perform_start: perform_start,
            succeeded:     succeeded
          )
        end

        begin
          Repository.delete_inflight!(active_job_id: job.job_id)
        rescue StandardError => e
          DispatchPolicy.config.logger&.warn("[dispatch_policy] failed to delete inflight row #{job.job_id}: #{e.class}: #{e.message}")
        end
      end
    end

    # Whether this job's executions belong in dispatch_policy_inflight_jobs.
    # The policy is the authority — a concurrency-family gate's admission
    # decision is a COUNT(*) over those rows, so they are not optional —
    # and the class-level opt-in only ADDS tracking for policies that have
    # no such gate.
    def self.tracking?(policy, job_class)
      return true if policy.inflight_tracked_gate

      job_class.respond_to?(:dispatch_policy_force_inflight_tracking) &&
        job_class.dispatch_policy_force_inflight_tracking
    end

    # Runs the job, then deletes any inflight row filed under its id.
    # Best-effort: a failure here must not turn a completed job into a
    # failed one.
    def self.release_after(job)
      yield
    ensure
      begin
        Repository.delete_inflight!(active_job_id: job.job_id)
      rescue StandardError => e
        DispatchPolicy.config.logger&.warn(
          "[dispatch_policy] failed to release inflight row #{job.job_id}: #{e.class}: #{e.message}"
        )
      end
    end

    # Deletes the inflight row for a job that ActiveJob discarded BEFORE
    # around_perform ran — most commonly an ActiveJob::DeserializationError
    # (a GlobalID whose record was deleted) on a job with
    # `discard_on ActiveJob::DeserializationError`. Argument deserialization
    # happens before the perform callbacks, so track's `ensure` never runs
    # and the row the Tick pre-inserted would otherwise sit until the
    # `inflight_queued_stale_after` sweeper reaps it (default 1h), holding a
    # concurrency slot the whole time. Wired to the `discard.active_job`
    # notification by the railtie. Idempotent: a no-op when no row exists
    # (e.g. discard fired after track already deleted it).
    # What the railtie's perform.active_job subscription does. A method
    # rather than a block in the initializer so the rule — reap only when
    # the perform actually failed — is reachable from a test; inverting it
    # inside an initializer block is invisible to the suite.
    def self.handle_failed_perform(event)
      return unless event.payload[:exception]

      handle_discard(event.payload[:job])
    end

    def self.handle_discard(job)
      return unless job
      return unless job.class.respond_to?(:dispatch_policy_name) && job.class.dispatch_policy_name

      Repository.delete_inflight!(active_job_id: job.job_id)
    rescue StandardError => e
      DispatchPolicy.config.logger&.warn(
        "[dispatch_policy] failed to clean up inflight row for discarded job #{job&.job_id}: #{e.class}: #{e.message}"
      )
    end

    # Reads the admitted_at column from the inflight row that the Tick
    # pre-inserted. Used as the start-of-queue-wait reference for the
    # adaptive_concurrency feedback signal (queue_lag = perform_start
    # - admitted_at). nil if the row vanished or the lookup fails —
    # the observation is then skipped.
    # Returns [admitted_at, partition_key] from the row the Tick
    # pre-inserted — the admission's own record of both facts.
    #
    # The key matters as much as the timestamp. The gate READS the
    # partition row's key, so an observation written under a key
    # recomputed from ctx files the AIMD state where the gate will never
    # look: after an edit to `partition_by`, observations accumulate on
    # the new key while `evaluate` keeps reading the old row. The
    # inflight row is the one place that already carries what the
    # admission decided.
    def self.lookup_admission(active_job_id)
      # Route through config.database_role: the inflight row lives in the
      # same DB the Tick pre-inserted it into, which under multi-DB is the
      # queue DB, not the default writing role of the worker process.
      result = Repository.with_connection do
        Repository.connection.exec_query(
          "SELECT admitted_at, partition_key FROM dispatch_policy_inflight_jobs " \
          "WHERE active_job_id = $1 LIMIT 1",
          "lookup_admission",
          [active_job_id]
        )
      end
      row = result.first
      return [nil, nil] unless row

      ts = row["admitted_at"]
      [ts.is_a?(Time) ? ts : Time.parse(ts.to_s), row["partition_key"]]
    rescue StandardError
      [nil, nil]
    end

    def self.record_adaptive_observations(policy:, gates:, partition_key:, admitted_at:, perform_start:, succeeded:)
      return if gates.empty?

      queue_lag_ms = if admitted_at
        ((perform_start - admitted_at) * 1000).to_i
      else
        # No admitted_at means we can't measure queue wait. Treat as 0
        # so the observation still increments sample_count and the
        # cap can grow if everything else is healthy.
        0
      end

      gates.each do |gate|
        gate.record_observation(
          policy_name:   policy.name,
          partition_key: partition_key,
          queue_lag_ms:  queue_lag_ms,
          succeeded:     succeeded
        )
      rescue StandardError => e
        DispatchPolicy.config.logger&.warn(
          "[dispatch_policy] adaptive observation failed for #{policy.name}/#{partition_key}: #{e.class}: #{e.message}"
        )
      end
    end

    # ----- heartbeat thread -----

    HEARTBEAT_KEY = :__dispatch_policy_heartbeat_token__

    Heartbeat = Struct.new(:thread, :stop_flag)

    def self.start_heartbeat(active_job_id)
      interval = DispatchPolicy.config.inflight_heartbeat_interval.to_f
      return nil if interval <= 0

      stop_flag = Concurrent::AtomicBoolean.new(false) if defined?(Concurrent::AtomicBoolean)
      stop_flag ||= ThreadSafeFlag.new

      thread = Thread.new do
        Thread.current.name = "dispatch_policy.heartbeat:#{active_job_id}"

        until stop_flag.true?
          # Sleep in small slices so stop is responsive without polling tight.
          slept = 0.0
          slice = [interval, 1.0].min
          while slept < interval && !stop_flag.true?
            sleep(slice)
            slept += slice
          end
          break if stop_flag.true?

          beat!(active_job_id)
        end
      end

      Heartbeat.new(thread, stop_flag)
    end

    # One beat, connection returned. Split out of the loop so a test can
    # drive it directly, and because the release below is the whole point.
    #
    # `Repository.with_connection` establishes config.database_role inside
    # this thread BEFORE the checkout: under multi-DB the pool must
    # resolve to the role's (where the inflight row lives), not the
    # default writing pool.
    #
    # The explicit release is NOT redundant with a nested
    # `connection_pool.with_connection`. A bare Thread.new runs outside
    # the Rails executor, so nothing has established a lease for it and
    # the pool treats its lease as PERMANENT: `with_connection` marks it
    # sticky and its ensure then skips `release_connection` precisely
    # because it assumes something outside owns it. Nothing does. The
    # first beat therefore pins a connection to the heartbeat thread for
    # the rest of the job, and when the thread dies the connection is not
    # returned either — it sits checked out with a dead owner until the
    # pool reaper gets to it. With a pool sized to the worker's thread
    # count (the Rails default: both come from RAILS_MAX_THREADS), every
    # tracked job that outlives one interval doubles its connection
    # demand, and the workers start raising ConnectionTimeoutError.
    def self.beat!(active_job_id)
      Repository.with_connection do
        Repository.heartbeat_inflight!(active_job_id: active_job_id)
      end
    rescue StandardError => e
      DispatchPolicy.config.logger&.warn("[dispatch_policy] heartbeat #{active_job_id} failed: #{e.class}: #{e.message}")
    ensure
      begin
        # Through with_connection, not bare: `connected_to` is block-scoped,
        # so by the time this ensure runs `current_role` is back to
        # :writing and `ActiveRecord::Base.connection_pool` resolves to the
        # WRITING pool — while the lease to hand back belongs to the role's
        # pool, where the inflight row lives. Releasing the wrong pool is
        # the same leak with an extra step.
        Repository.with_connection { Repository.base_class.connection_pool.release_connection }
      rescue StandardError
        # A pool that has gone away takes its connections with it.
      end
    end

    def self.stop_heartbeat(heartbeat)
      return if heartbeat.nil?

      heartbeat.stop_flag.make_true
      # Wake the thread out of any in-progress sleep so we don't wait the full slice.
      heartbeat.thread.wakeup if heartbeat.thread.alive?
      heartbeat.thread.join(1.0)
    rescue StandardError
      # Worst case: the thread is killed by GC; the inflight row gets a stale
      # heartbeat_at and the sweeper will reclaim it after inflight_stale_after.
    end

    # Tiny fallback if concurrent-ruby isn't available (it's a Rails dep
    # via active_support so it normally is).
    class ThreadSafeFlag
      def initialize; @mutex = Mutex.new; @value = false; end
      def true?; @mutex.synchronize { @value }; end
      def make_true; @mutex.synchronize { @value = true }; end
    end
  end
end
