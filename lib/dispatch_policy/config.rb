# frozen_string_literal: true

module DispatchPolicy
  class Config
    attr_accessor :tick_max_duration,
                  :partition_batch_size,
                  :admission_batch_size,
                  :idle_pause,
                  :partition_inactive_after,
                  :inflight_stale_after,
                  :inflight_heartbeat_interval,
                  :real_adapter,
                  :logger,
                  :clock,
                  :sweep_every_ticks,
                  :metrics_retention,
                  :database_role,
                  :fairness_half_life_seconds,
                  :tick_admission_budget

    def initialize
      @tick_max_duration         = 25
      @partition_batch_size      = 50
      @admission_batch_size      = 100
      @idle_pause                = 0.5
      @partition_inactive_after  = 24 * 60 * 60
      @inflight_stale_after      = 5 * 60
      @inflight_heartbeat_interval = 30
      @real_adapter              = nil
      @logger                    = nil
      @clock                     = -> { Time.now.utc }
      @sweep_every_ticks         = 50
      @metrics_retention         = 24 * 60 * 60
      # AR role for the admission TX. nil = default connection. Set to
      # e.g. :queue when the host runs solid_queue on a separate DB.
      @database_role             = nil
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
