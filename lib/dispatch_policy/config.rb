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
                  :metrics_retention

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
