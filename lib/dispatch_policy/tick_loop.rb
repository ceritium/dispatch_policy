# frozen_string_literal: true

module DispatchPolicy
  # Shared driver for DispatchTickLoopJob and any foreground tick (e.g. a
  # rake task). Reaps once at startup, then loops Tick.run with an
  # interruptible sleep and bails when stop_when returns true.
  #
  # Tick.reap recovers in-flight counters from workers that crashed
  # mid-perform — a rare event. Running it once per TickLoop invocation
  # is enough because the typical deployment chains a fresh TickLoop
  # every tick_max_duration (default 60s) plus a 1s self-chain wait, so
  # in practice reap fires every ~minute. If you need faster recovery,
  # call DispatchPolicy::Tick.reap from a dedicated cron job.
  class TickLoop
    def self.run(policy_name: nil, sleep_for: nil, sleep_for_busy: nil, stop_when: -> { false })
      idle_sleep = (sleep_for      || DispatchPolicy.config.tick_sleep).to_f
      busy_sleep = (sleep_for_busy || DispatchPolicy.config.tick_sleep_busy).to_f

      begin
        ActiveRecord::Base.uncached { Tick.reap }
      rescue StandardError => e
        Rails.logger&.error("[DispatchPolicy] reap error: #{e.class}: #{e.message}")
        Rails.error.report(e, handled: true) if defined?(Rails) && Rails.respond_to?(:error)
      end

      loop do
        break if stop_when.call

        admitted = 0
        begin
          ActiveRecord::Base.uncached do
            admitted = Tick.run(policy_name: policy_name).to_i
          end
        rescue StandardError => e
          Rails.logger&.error("[DispatchPolicy] tick error: #{e.class}: #{e.message}")
          Rails.error.report(e, handled: true) if defined?(Rails) && Rails.respond_to?(:error)
        end

        break if stop_when.call

        interruptible_sleep(admitted.positive? ? busy_sleep : idle_sleep, stop_when)
      end
    end

    def self.interruptible_sleep(total, stop_when)
      return unless total.positive?

      remaining = total
      step = 0.1
      while remaining.positive?
        break if stop_when.call
        chunk = [ remaining, step ].min
        sleep(chunk)
        remaining -= chunk
      end
    end
  end
end
