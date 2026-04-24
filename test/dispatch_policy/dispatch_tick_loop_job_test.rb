# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class DispatchTickLoopJobTest < ActiveJob::TestCase
    setup do
      @orig_duration   = DispatchPolicy.config.tick_max_duration
      @orig_tick_sleep = DispatchPolicy.config.tick_sleep
      @orig_tick_busy  = DispatchPolicy.config.tick_sleep_busy
      DispatchPolicy.config.tick_max_duration = 0
      DispatchPolicy.config.tick_sleep        = 0
      DispatchPolicy.config.tick_sleep_busy   = 0
    end

    teardown do
      DispatchPolicy.config.tick_max_duration = @orig_duration
      DispatchPolicy.config.tick_sleep        = @orig_tick_sleep
      DispatchPolicy.config.tick_sleep_busy   = @orig_tick_busy
    end

    test "perform runs the tick loop and re-enqueues itself" do
      assert_enqueued_with(job: DispatchTickLoopJob) do
        DispatchTickLoopJob.perform_now
      end
    end
  end
end
