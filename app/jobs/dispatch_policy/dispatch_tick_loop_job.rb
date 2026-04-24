# frozen_string_literal: true

module DispatchPolicy
  # Long-running tick. Loops Tick.reap + Tick.run up to tick_max_duration,
  # sleeping tick_sleep (adaptive: tick_sleep_busy after productive ticks)
  # between iterations. Self-enqueues at the end of perform so the next
  # run starts immediately.
  class DispatchTickLoopJob < ActiveJob::Base
    queue_as :default

    def perform(policy_name = nil)
      deadline = Time.current + DispatchPolicy.config.tick_max_duration

      DispatchPolicy::TickLoop.run(
        policy_name: policy_name,
        stop_when:   -> {
          (defined?(GoodJob) && GoodJob.respond_to?(:current_thread_shutting_down?) && GoodJob.current_thread_shutting_down?) ||
            Time.current >= deadline
        }
      )

      # Self-chain so we don't depend on a cron for the next iteration.
      DispatchTickLoopJob.set(wait: 1.second).perform_later(policy_name)
    end
  end
end
