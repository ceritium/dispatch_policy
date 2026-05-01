# frozen_string_literal: true

module DispatchPolicy
  # Drives admission until `stop_when` fires (deadline, shutdown signal, etc).
  # Runs one Tick per policy per loop iteration; sleeps `idle_pause` when no
  # jobs were admitted across all policies. Periodically (every
  # `sweep_every_ticks` iterations) sweeps stale inflight rows and inactive
  # partitions.
  module TickLoop
    module_function

    # @param policy_name [String, nil] limit to one policy. nil = all registered.
    # @param shard [String, nil] limit to one shard. nil = all shards.
    def run(policy_name: nil, shard: nil, stop_when: -> { false })
      config       = DispatchPolicy.config
      logger       = config.logger
      iteration    = 0

      loop do
        break if stop_when.call

        names = policy_names(policy_name)
        if names.empty?
          sleep(config.idle_pause)
          next
        end

        admitted = 0
        names.each do |name|
          break if stop_when.call

          begin
            result = Tick.run(policy_name: name, shard: shard)
            admitted += result.jobs_admitted
          rescue StandardError => e
            logger&.error("[dispatch_policy] tick error policy=#{name} shard=#{shard.inspect} #{e.class}: #{e.message}\n#{e.backtrace.first(10).join("\n")}")
          end
        end

        iteration += 1
        if (iteration % config.sweep_every_ticks).zero?
          sweep!
        end

        sleep(config.idle_pause) if admitted.zero?
      end
    end

    def policy_names(filter)
      if filter
        [filter.to_s]
      else
        DispatchPolicy.registry.names
      end
    end

    def sweep!
      cfg = DispatchPolicy.config
      Repository.sweep_stale_inflight!(cutoff_seconds: cfg.inflight_stale_after)
      Repository.sweep_inactive_partitions!(cutoff_seconds: cfg.partition_inactive_after)
      Repository.sweep_old_tick_samples!(cutoff_seconds: cfg.metrics_retention)
    rescue StandardError => e
      DispatchPolicy.config.logger&.error("[dispatch_policy] sweep error: #{e.class}: #{e.message}")
    end
  end
end
