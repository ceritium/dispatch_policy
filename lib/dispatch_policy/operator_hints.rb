# frozen_string_literal: true

module DispatchPolicy
  # Pure-Ruby hint generator. Takes a snapshot of live metrics and
  # returns a list of {level:, message:} that the dashboard renders.
  #
  # Each predicate is intentionally conservative: hints fire on
  # crossings the operator can fix from the UI or by toggling a config
  # value, not on noise. Levels:
  #
  #   :info     — observation worth glancing at
  #   :warn     — attention soon
  #   :critical — fix now
  module OperatorHints
    Hint = Struct.new(:level, :message, keyword_init: true)

    module_function

    # `metrics` is a hash of:
    #   tick_max_duration_ms:   int  (config tick_max_duration × 1000)
    #   avg_tick_ms:            int
    #   max_tick_ms:            int
    #   pending_total:          int
    #   admitted_per_minute:    int  (last 1m)
    #   forward_failures:       int  (last 1m)
    #   jobs_admitted:          int  (last 1m, denominator for fail %)
    #   active_partitions:      int
    #   never_checked:          int
    #   in_backoff:             int
    #   total_partitions:       int
    #   adapter_target_jps:     int|nil  (config.adapter_throughput_target)
    def for(metrics)
      hints = []
      m     = metrics

      # ---- tick approaching deadline ---------------------------------
      if m[:tick_max_duration_ms].to_i.positive? && m[:avg_tick_ms].to_i.positive?
        ratio = m[:avg_tick_ms].to_f / m[:tick_max_duration_ms]
        if ratio >= 0.6
          hints << Hint.new(
            level: ratio >= 0.85 ? :critical : :warn,
            message: "Avg tick is #{format('%.0f%%', ratio * 100)} of tick_max_duration. " \
                     "Lower admission_batch_size, set tick_admission_budget, or shard the policy."
          )
        end
      end

      # ---- backlog drain time ----------------------------------------
      if m[:admitted_per_minute].to_i.positive? && m[:pending_total].to_i.positive?
        drain_minutes = m[:pending_total].to_f / m[:admitted_per_minute]
        if drain_minutes >= 30
          level = drain_minutes >= 120 ? :warn : :info
          hints << Hint.new(
            level: level,
            message: "At #{m[:admitted_per_minute]} admits/min, the current backlog of " \
                     "#{m[:pending_total]} pending would take ~#{drain_minutes.round} min " \
                     "to drain. Raise admission_batch_size, raise the gate's rate, or shard."
          )
        end
      end

      # ---- pending growing while admit rate is non-trivial -----------
      if m[:pending_trend] == :up && m[:admitted_per_minute].to_i.positive?
        hints << Hint.new(
          level: :warn,
          message: "Pending is growing while we are admitting. Inflow > outflow — " \
                   "either the throttle rate is below the producer rate, or the worker pool can't keep up."
        )
      end

      # ---- forward failure rate --------------------------------------
      if m[:jobs_admitted].to_i.positive?
        rate = m[:forward_failures].to_f / m[:jobs_admitted]
        if rate >= 0.01
          hints << Hint.new(
            level: rate >= 0.05 ? :critical : :warn,
            message: "Forward failures at #{format('%.1f%%', rate * 100)} (#{m[:forward_failures]} / " \
                     "#{m[:jobs_admitted]} admits). Inspect logs — adapter is rejecting enqueues."
          )
        end
      end

      # ---- never_checked > 0 with cardinality > batch ----------------
      if m[:never_checked].to_i.positive?
        hints << Hint.new(
          level: :warn,
          message: "#{m[:never_checked]} active partitions have never been checked. " \
                   "The tick is not getting through them — increase partition_batch_size or shard."
        )
      end

      # ---- partition cardinality -------------------------------------
      if m[:total_partitions].to_i >= 50_000
        hints << Hint.new(
          level: :info,
          message: "#{m[:total_partitions]} partitions in DB. claim_partitions starts to feel " \
                   "this around 50k–100k. Consider lowering partition_inactive_after to GC " \
                   "drained ones earlier."
        )
      end

      # ---- adapter ceiling proximity --------------------------------
      target_jps = m[:adapter_target_jps].to_i
      if target_jps.positive? && m[:admitted_per_minute].to_i.positive?
        current_jps = m[:admitted_per_minute] / 60.0
        ratio = current_jps / target_jps
        if ratio >= 0.7
          hints << Hint.new(
            level: ratio >= 0.95 ? :critical : :warn,
            message: "Admitting #{format('%.0f', current_jps)} jobs/sec, " \
                     "#{format('%.0f%%', ratio * 100)} of the configured adapter ceiling " \
                     "(#{target_jps}/sec). Consider an additional shard before the next traffic spike."
          )
        end
      end

      hints
    end
  end
end
