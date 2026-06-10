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

    class_methods do
      def dispatch_policy_inflight_tracking
        around_perform do |job, block|
          DispatchPolicy::InflightTracker.track(job, &block)
        end
      end
    end

    def self.track(job)
      policy_name = job.class.respond_to?(:dispatch_policy_name) && job.class.dispatch_policy_name
      return yield unless policy_name

      policy = DispatchPolicy.registry.fetch(policy_name)
      return yield unless policy

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
      admitted_at    = nil
      perform_start  = nil
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

        admitted_at   = adaptive_gates.any? ? lookup_admitted_at(job.job_id) : nil
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
            partition_key: partition_key,
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
    def self.lookup_admitted_at(active_job_id)
      # Route through config.database_role: the inflight row lives in the
      # same DB the Tick pre-inserted it into, which under multi-DB is the
      # queue DB, not the default writing role of the worker process.
      result = Repository.with_connection do
        ActiveRecord::Base.connection.exec_query(
          "SELECT admitted_at FROM dispatch_policy_inflight_jobs WHERE active_job_id = $1 LIMIT 1",
          "lookup_admitted_at",
          [active_job_id]
        )
      end
      row = result.first
      return nil unless row
      ts = row["admitted_at"]
      ts.is_a?(Time) ? ts : Time.parse(ts.to_s)
    rescue StandardError
      nil
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

          begin
            # Establish config.database_role inside this thread BEFORE the
            # checkout: under multi-DB, connection_pool must resolve to the
            # role's pool (where the inflight row lives), not the default
            # writing pool. with_connection swaps the role thread-locally;
            # the nested pool checkout then borrows/returns a dedicated
            # connection from that pool per beat.
            Repository.with_connection do
              ActiveRecord::Base.connection_pool.with_connection do
                Repository.heartbeat_inflight!(active_job_id: active_job_id)
              end
            end
          rescue StandardError => e
            DispatchPolicy.config.logger&.warn("[dispatch_policy] heartbeat #{active_job_id} failed: #{e.class}: #{e.message}")
          end
        end
      end

      Heartbeat.new(thread, stop_flag)
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
