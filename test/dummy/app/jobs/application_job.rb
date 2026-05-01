# frozen_string_literal: true

class ApplicationJob < ActiveJob::Base
  include DispatchPolicy::InflightTracker
end
