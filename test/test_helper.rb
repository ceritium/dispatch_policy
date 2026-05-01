# frozen_string_literal: true

ENV["RAILS_ENV"] ||= "test"

require "minitest/autorun"
require "active_support/testing/time_helpers"

require "active_record"
require "active_job"
require "active_job/test_helper"

# Activate the inline ActiveJob adapter by default for tests that don't talk to DB.
ActiveJob::Base.queue_adapter = :test

require_relative "../lib/dispatch_policy"

# Database is only required for repository / integration tests.
def with_test_db(&block)
  ActiveRecord::Base.establish_connection(
    adapter:  "postgresql",
    encoding: "unicode",
    host:     ENV.fetch("DB_HOST", "localhost"),
    username: ENV.fetch("DB_USER", ENV["USER"]),
    password: ENV.fetch("DB_PASS", ""),
    database: ENV.fetch("DB_NAME", "dispatch_policy_test")
  )
  yield
end

module DispatchPolicy
  module TestHelpers
    def reset_dispatch_policy!
      DispatchPolicy.reset_config!
      DispatchPolicy.reset_registry!
    end
  end
end

class Minitest::Test
  include DispatchPolicy::TestHelpers
  include ActiveSupport::Testing::TimeHelpers

  def setup
    reset_dispatch_policy!
  end
end
