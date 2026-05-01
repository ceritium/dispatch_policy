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

      ctx              = policy.build_context(job.arguments, queue_name: job.queue_name&.to_s)
      concurrency_gate = policy.gates.find { |g| g.name == :concurrency }

      partition_key = if concurrency_gate
        concurrency_gate.inflight_partition_key(policy.name, ctx)
      else
        policy.partition_key_for(ctx)
      end

      Repository.insert_inflight!([{
        policy_name:    policy.name,
        partition_key:  partition_key,
        active_job_id:  job.job_id
      }])

      heartbeat = start_heartbeat(job.job_id)

      begin
        yield
      ensure
        stop_heartbeat(heartbeat)
        begin
          Repository.delete_inflight!(active_job_id: job.job_id)
        rescue StandardError => e
          DispatchPolicy.config.logger&.warn("[dispatch_policy] failed to delete inflight row #{job.job_id}: #{e.class}: #{e.message}")
        end
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
            ActiveRecord::Base.connection_pool.with_connection do
              Repository.heartbeat_inflight!(active_job_id: active_job_id)
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
