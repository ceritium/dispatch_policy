# frozen_string_literal: true

module DispatchPolicy
  class ApplicationController < ActionController::Base
    protect_from_forgery with: :exception

    helper_method :format_time, :format_count, :registered_policies

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
  end
end
