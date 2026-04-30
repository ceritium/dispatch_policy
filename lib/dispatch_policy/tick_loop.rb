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

      reload_policy_configs!(policy_name)
      maybe_auto_tune!(policy_name)

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

    # Reload per-policy config overrides from the DB-backed table
    # (or, in :code mode, mirror the DSL back into the DB) so each
    # TickLoop reflects the latest operator tunes.
    def self.reload_policy_configs!(policy_name)
      names = policy_name ? [ policy_name ] : DispatchPolicy.registry.keys
      names.each do |name|
        job_class = DispatchPolicy.registry[name]
        policy = job_class&.resolved_dispatch_policy
        next unless policy
        policy.reload_overrides_from_db!
      end
    rescue ActiveRecord::StatementInvalid,
           ActiveRecord::ConnectionNotEstablished,
           ActiveRecord::NoDatabaseError
      # Migration not run yet — skip silently. The TickLoop should
      # still be able to operate with code-side defaults.
    end

    # Closed-loop self-tuning. When config.auto_tune == :apply, each
    # TickLoop boot runs Stats.bottleneck for every round-robin
    # policy and persists the recommended_config back to the DB
    # (source: "auto"). The next reload_policy_configs! picks them
    # up. :recommend logs without writing; false is a no-op.
    def self.maybe_auto_tune!(policy_name)
      mode = DispatchPolicy.config.auto_tune
      return unless mode

      names = policy_name ? [ policy_name ] : DispatchPolicy.registry.keys
      names.each do |name|
        result = Stats.bottleneck(name)
        recommended = result[:recommended_config]
        next if recommended.nil? || recommended.empty?

        flat = recommended.transform_values { |h| h.is_a?(Hash) ? h[:suggested] : h }.compact

        if mode == :apply
          DispatchPolicy::PolicyConfig.upsert_many!(
            policy_name: name,
            values:      flat,
            source:      "auto"
          )
          job_class = DispatchPolicy.registry[name]
          job_class&.resolved_dispatch_policy&.reload_overrides_from_db!
          Rails.logger&.info("[DispatchPolicy] auto_tune applied to #{name}: #{flat.inspect}")
        else
          Rails.logger&.info("[DispatchPolicy] auto_tune recommends for #{name}: #{flat.inspect}")
        end
      end
    rescue ActiveRecord::StatementInvalid,
           ActiveRecord::ConnectionNotEstablished,
           ActiveRecord::NoDatabaseError
      # Same rationale as reload_policy_configs!.
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
