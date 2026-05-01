# frozen_string_literal: true

module DispatchPolicy
  class ApplicationController < ActionController::Base
    protect_from_forgery with: :exception

    helper_method :format_time, :format_count, :format_duration_seconds,
                  :format_duration_ms, :sparkline, :registered_policies

    private

    def registered_policies
      DispatchPolicy.registry.each.to_a
    end

    def format_time(time)
      return "—" unless time
      time.utc.strftime("%Y-%m-%d %H:%M:%S")
    end

    def format_count(value)
      return "0" if value.nil?
      value.to_i.to_s.reverse.scan(/\d{1,3}/).join(",").reverse
    end

    def format_duration_seconds(seconds)
      return "—" if seconds.nil?
      s = seconds.to_f
      return "%.0fms" % (s * 1000) if s < 1
      return "%.1fs"  % s          if s < 60
      return "%.1fm"  % (s / 60)   if s < 3600
      "%.1fh" % (s / 3600)
    end

    def format_duration_ms(ms)
      return "—" if ms.nil?
      format_duration_seconds(ms.to_f / 1000.0)
    end

    BARS = %w[▁ ▂ ▃ ▄ ▅ ▆ ▇ █].freeze

    def sparkline(values, width: 30)
      return "" if values.nil? || values.empty?

      data = values.map(&:to_i)
      data = data.last(width)
      max  = data.max
      return BARS.first * data.size if max.nil? || max.zero?

      data.map { |v| BARS[((v.to_f / max) * (BARS.size - 1)).round] }.join
    end
  end
end
