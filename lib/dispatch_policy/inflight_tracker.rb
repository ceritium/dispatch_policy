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
      queue_lag_ms    = nil
      observation_key = nil
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
          # This IS the measurement of the queue wait, and it happens here —
          # before `yield` — so perform duration cannot leak into the
          # signal. The lag is computed by the database rather than
          # subtracted from `Time.current`; see `lookup_admission`.
          queue_lag_ms, admitted_key = lookup_admission(job.job_id)
          # Fall back to the recomputed key only when the row is gone.
          observation_key = admitted_key || partition_key
        end
        heartbeat = start_heartbeat(job.job_id)

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
            queue_lag_ms:  queue_lag_ms,
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

    # Returns [queue_lag_ms, partition_key] from the inflight row the Tick
    # pre-inserted — the admission's own record of both facts. `[nil, nil]`
    # when the row is gone or the lookup fails, and the caller then records
    # the observation with a lag of 0 rather than dropping it.
    #
    # A10: the LAG IS COMPUTED BY THE DATABASE, not by subtracting
    # `admitted_at` from the worker's `Time.current`. Those are two
    # different clocks — `admitted_at` is written by Postgres `now()` on
    # the tick process's connection — and the adaptive gate is an AIMD
    # controller whose whole input is this number: a host running a few
    # hundred milliseconds fast against `target_lag_ms` reads every job as
    # late, shrinks `current_max` on every observation and never grows it
    # back, so the cap collapses to `min` and stays there with nothing
    # anywhere reporting a clock problem. A skew ALSO comes from the two
    # ends disagreeing about the session TimeZone, since these columns are
    # `timestamp WITHOUT time zone` — hence `clock_timestamp()::timestamp`,
    # which lands in the same frame `now()` stored.
    #
    # `clock_timestamp()`, not `now()`: `now()` is the TRANSACTION
    # timestamp, so inside a host that wraps the perform in a transaction
    # (Rails transactional tests, among others) it stops advancing and the
    # lag becomes "time since that transaction opened".
    #
    # The lag is clamped at 0 because the gate treats it as a duration —
    # but the clamp happens in RUBY, after a warning, rather than in SQL.
    # A tick and a worker whose sessions disagree about the TimeZone hand
    # this a negative number, and clamping it silently turns "the two ends
    # disagree" into "the job waited no time at all", which is exactly what
    # a healthy fast job looks like: the cap then grows on a measurement
    # that means nothing, with nothing anywhere to notice. The opposite
    # direction of the same mismatch announces itself by collapsing the cap
    # to `min`, so it never needed the warning.
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
          "SELECT partition_key, EXTRACT(EPOCH FROM " \
          "(clock_timestamp()::timestamp - admitted_at)) * 1000 AS raw_lag_ms " \
          "FROM dispatch_policy_inflight_jobs WHERE active_job_id = $1 LIMIT 1",
          "lookup_admission",
          [active_job_id]
        )
      end
      row = result.first
      return [nil, nil] unless row

      lag = row["raw_lag_ms"].to_f
      if lag.negative?
        # The one direction of a session-TimeZone mismatch that leaves no
        # trace otherwise: clamped to 0 it is indistinguishable from a job
        # that waited no time at all, and the AIMD cap then grows on a
        # measurement that means nothing. The other direction announces
        # itself by collapsing the cap. Say so once per observation.
        DispatchPolicy.config.logger&.warn(
          "[dispatch_policy] negative queue lag (#{lag.round}ms) for #{active_job_id}: the " \
          "session that wrote admitted_at and this one disagree about TimeZone, so the " \
          "adaptive cap is being tuned on an unreliable signal"
        )
      end
      [[lag, 0.0].max.to_i, row["partition_key"]]
    rescue StandardError
      [nil, nil]
    end

    def self.record_adaptive_observations(policy:, gates:, partition_key:, queue_lag_ms:, succeeded:)
      return if gates.empty?

      # A nil lag means the inflight row was gone or the lookup failed, so
      # the queue wait is unknown. Record 0 rather than dropping the
      # observation: sample_count still advances and the cap can grow if
      # everything else is healthy.
      queue_lag_ms ||= 0

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
    #
    # ONE thread per process, beating every job this process is running.
    #
    # A12: it used to be one thread per running job, each checking out its
    # own connection. The Rails default sizes the pool to the worker's
    # thread count (both come from RAILS_MAX_THREADS), and a performing job
    # holds its connection for the whole perform — the executor only
    # returns it when the job finishes. A saturated worker therefore has
    # every connection held and every beat queued behind `checkout_timeout`,
    # which raises, is swallowed as best-effort, and leaves `heartbeat_at`
    # standing still. That is precisely what the stale sweeper reaps: it
    # deletes the inflight row of a job that is still running, and the
    # concurrency gate re-admits against a slot that is still occupied —
    # the cap it exists to enforce silently lapses under exactly the load
    # that makes it matter. One thread needs one connection for one
    # statement per interval, whatever the worker is doing.
    #
    # The registry is a plain Hash under a Mutex. Thread lifecycle is
    # decided under that same Mutex, which is what keeps a job registering
    # from racing the thread's decision to exit when the set empties.

    HEARTBEAT_THREAD_NAME = "dispatch_policy.heartbeat"

    # What `start_heartbeat` hands back and `stop_heartbeat` takes. NOT the
    # active_job_id: the registry maps an id to the sequence numbers of the
    # EXECUTIONS currently registered under it, and both halves of that
    # matter.
    #
    # A count, because an id is not unique in time OR in parallel. At-least-once
    # delivery can put two deliveries of one job on the same worker, and
    # `stop_heartbeat` on the first would otherwise unregister the second —
    # a regression this rewrite introduced, since a thread per execution
    # could not do that to its sibling. (Their inflight ROW is shared
    # either way, which is an older and larger problem: `insert_inflight!`
    # is ON CONFLICT DO NOTHING and `delete_inflight!` keys on
    # active_job_id, so the first to finish deletes the other's row too.
    # Fixing that needs a per-execution key in the forwarded payload.)
    #
    # A sequence, because ActiveJob KEEPS the job_id across retries, so
    # "the same id leaves and comes back" is not exotic — it is what
    # `retry_on` does. The beat's pruning compares against a snapshot taken
    # before the UPDATE, and without a sequence a retry that registers in
    # that window is pruned by the answer to a question about its
    # predecessor: the live execution then never beats again, and
    # `inflight_stale_after` later has the sweeper delete its row while it
    # runs. Reproduced by widening the beat, with the loop, the snapshot
    # and the pruning untouched.
    Registration = Struct.new(:active_job_id, :seq)

    @heartbeat_mutex  = Mutex.new
    @heartbeat_ids    = {}
    @heartbeat_seq    = 0
    @heartbeat_thread = nil
    @heartbeat_pid    = nil

    class << self
      attr_reader :heartbeat_mutex, :heartbeat_ids
    end

    # Registers a job for beating and returns the token `stop_heartbeat`
    # takes. nil when the heartbeat is disabled.
    def self.start_heartbeat(active_job_id)
      interval = DispatchPolicy.config.inflight_heartbeat_interval.to_f
      return nil if interval <= 0

      heartbeat_mutex.synchronize do
        forget_inherited_registrations!
        @heartbeat_seq += 1
        (heartbeat_ids[active_job_id] ||= []) << @heartbeat_seq
        ensure_heartbeat_thread
        Registration.new(active_job_id, @heartbeat_seq)
      end
    end

    # Caller holds heartbeat_mutex.
    #
    # A fork copies the registry but NOT the thread. Every id in the copy
    # belongs to a job running in the PARENT, and beating them from the
    # child is worse than not beating them at all: it keeps the inflight
    # row of a job that is not running here fresh, so the stale sweeper
    # never reclaims it and the concurrency slot is lost for as long as the
    # child lives. Reproduced with a real fork — the child beat the
    # parent's job 2.6s after the parent had stopped.
    def self.forget_inherited_registrations!
      return if @heartbeat_pid.nil? || @heartbeat_pid == Process.pid

      heartbeat_ids.clear
      @heartbeat_thread = nil
      @heartbeat_pid    = nil
    end

    def self.stop_heartbeat(token)
      return if token.nil?

      heartbeat_mutex.synchronize do
        seqs = heartbeat_ids[token.active_job_id]
        next if seqs.nil?

        seqs.delete(token.seq)
        heartbeat_ids.delete(token.active_job_id) if seqs.empty?
      end
    end

    # Caller holds heartbeat_mutex.
    #
    # Restarts after a fork as well as after a crash: a forked child
    # inherits no threads, so a worker that forks (or a Puma process that
    # preloads the app) would otherwise register jobs against a thread
    # that does not exist in this process.
    def self.ensure_heartbeat_thread
      return if @heartbeat_thread && @heartbeat_pid == Process.pid && @heartbeat_thread.alive?

      @heartbeat_pid = Process.pid
      thread = Thread.new { heartbeat_loop }
      # Named from HERE, not from inside the thread: a name set in the
      # thread body is not there yet when this method returns, and the
      # name is how an operator finds it in a thread dump.
      thread.name       = HEARTBEAT_THREAD_NAME
      @heartbeat_thread = thread
    end

    # The interval is re-read every iteration rather than captured at
    # startup. This thread outlives any one job, so a captured value would
    # pin the cadence to whatever the config happened to say when the
    # process ran its first tracked job — and setting the interval to 0
    # (the documented way to turn the heartbeat off) would leave the thread
    # beating forever.
    # How long the loop waits after an error before trying again. It does
    # NOT exit on one: with a thread per job an error cost one job's
    # heartbeat, and with one thread it would cost every running job in the
    # process — they stay registered, nothing beats them, and
    # `inflight_stale_after` later has the sweeper delete the rows of jobs
    # that are still running. Only a NEW registration would have restarted
    # it, and a worker saturated with long jobs does not produce one.
    # Reproduced with a non-numeric `inflight_heartbeat_interval`: three
    # registered jobs, no live thread, nothing beating them.
    HEARTBEAT_ERROR_BACKOFF = 1.0

    def self.heartbeat_loop
      loop do
        begin
          interval = DispatchPolicy.config.inflight_heartbeat_interval.to_f
          # 0 is the documented way to turn the heartbeat off; a thread that
          # captured the interval at startup would keep beating forever.
          break if retire? { interval <= 0 }

          sleep_interval(interval)

          # The snapshot carries the sequence numbers, not just the ids:
          # the pruning below has to be able to tell "this id is gone" from
          # "this id left and a retry registered it again while the beat
          # was in flight".
          snapshot = heartbeat_mutex.synchronize { heartbeat_ids.transform_values(&:dup) }
          ids = snapshot.keys
          if ids.empty?
            break if retire? { heartbeat_ids.empty? }

            next
          end

          # `beat!` answers with the ids that still had a row — or nil if it
          # could not talk to the database at all, and nil is "we learned
          # nothing", NOT "no rows survived". Reading it as an empty list
          # unregisters every running job in the process on one transient
          # failure, for good, and five minutes later the sweeper deletes
          # their rows while they run on. Anything genuinely missing from a
          # SUCCESSFUL beat has been deleted — the job finished and
          # something killed the thread before `track`'s ensure could
          # unregister it, or the sweeper reclaimed the row — so drop it
          # rather than carry it in every beat for the life of the process.
          alive = beat!(ids)
          next if alive.nil?

          gone = ids - alive
          if gone.any?
            heartbeat_mutex.synchronize do
              # Only when nothing re-registered in the meantime. Otherwise
              # a retry that arrived while the UPDATE was in flight is
              # unregistered by the answer to a question about the
              # execution it replaced.
              gone.each { |id| heartbeat_ids.delete(id) if heartbeat_ids[id] == snapshot[id] }
            end
          end
        rescue StandardError => e
          DispatchPolicy.config.logger&.warn(
            "[dispatch_policy] heartbeat cycle failed, retrying in " \
            "#{HEARTBEAT_ERROR_BACKOFF}s: #{e.class}: #{e.message}"
          )
          sleep HEARTBEAT_ERROR_BACKOFF
        end
      end
    rescue Exception => e # rubocop:disable Lint/RescueException
      # Only reachable for what the per-cycle rescue above does not catch —
      # a Thread#raise, an Exception subclass outside StandardError. Drop
      # the handle (and only ours) so the next `start_heartbeat` installs a
      # fresh thread, then let it propagate.
      retire? { true }
      DispatchPolicy.config.logger&.error(
        "[dispatch_policy] heartbeat thread died: #{e.class}: #{e.message}. " \
        "Running jobs will not be heartbeated until another job starts."
      )
      raise
    end

    # Evaluates the exit condition and clears the handle in ONE critical
    # section. Checking from the other side cannot work: the thread is
    # still `alive?` while it is on its way out, so a job registering at
    # that moment would find a thread that is about to stop. Under this
    # lock it either arrives first — the condition then sees its id and
    # the thread carries on — or finds the handle already nil and starts a
    # fresh thread.
    def self.retire?
      heartbeat_mutex.synchronize do
        next false unless yield

        # Only ever retire OURSELVES: after a fork the handle can already
        # belong to the child's new thread.
        @heartbeat_thread = nil if @heartbeat_thread == Thread.current
        true
      end
    end

    # Sleeps up to `interval`, in slices of at most a second, re-reading
    # the configured interval as it goes.
    #
    # This thread outlives every job it beats, so the cadence cannot be
    # fixed at the moment the process happened to run its first tracked
    # job: lowering `inflight_heartbeat_interval` — or setting it to 0 to
    # turn the heartbeat off — would otherwise have to wait out the old
    # value, which at the default is half a minute of beating nobody asked
    # for. The slice also keeps the thread's own retirement prompt.
    def self.sleep_interval(interval)
      slept = 0.0
      while slept < interval
        slice = [interval - slept, 1.0].min
        sleep(slice)
        slept += slice

        current = DispatchPolicy.config.inflight_heartbeat_interval.to_f
        break if current <= 0 || slept >= current
      end
    end

    # One beat for every id given, connection returned. Split out of the
    # loop so a test can drive it directly, and because the release below
    # is the whole point.
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
    # beat would therefore pin a connection to the heartbeat thread for
    # the life of the process, and when the thread dies the connection is
    # not returned either — it sits checked out with a dead owner until
    # the pool reaper gets to it. Now that there is one thread rather than
    # one per job that is a single connection instead of the whole pool
    # over again, which makes it cheaper to get wrong and no less wrong.
    def self.beat!(active_job_ids)
      ids = Array(active_job_ids)
      return [] if ids.empty?

      Repository.with_connection do
        Repository.heartbeat_inflight!(active_job_ids: ids)
      end
    rescue ActiveRecord::ConnectionTimeoutError => e
      # Named separately because it is the one failure with a fix, and a
      # `warn` indistinguishable from any other made it invisible. One
      # thread needs ONE connection; if it cannot get that, the pool has no
      # spare above the worker's concurrency, every beat is lost, and the
      # stale sweeper starts deleting the inflight rows of jobs that are
      # still running — the concurrency cap then admits over them.
      DispatchPolicy.config.logger&.error(
        "[dispatch_policy] heartbeat could not get a connection (#{e.class}): #{ids.size} " \
        "running job(s) are not being heartbeated and will be swept as stale. The pool must " \
        "have at least one connection above the worker's thread count."
      )
      nil
    rescue StandardError => e
      DispatchPolicy.config.logger&.warn("[dispatch_policy] heartbeat #{ids.size} row(s) failed: #{e.class}: #{e.message}")
      nil
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

  end
end
