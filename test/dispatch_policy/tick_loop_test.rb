# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class TickLoopTest < ActiveSupport::TestCase
    test "stops immediately when stop_when is always true" do
      iterations = 0
      TickLoop.run(
        sleep_for:      0,
        sleep_for_busy: 0,
        stop_when: -> {
          iterations += 1
          iterations >= 1
        }
      )
      assert_equal 1, iterations
    end

    test "interruptible_sleep returns early when stop_when flips" do
      flag = false
      Thread.new { sleep 0.05; flag = true }
      start = Time.current
      TickLoop.interruptible_sleep(5, -> { flag })
      elapsed = Time.current - start
      assert elapsed < 1.0, "expected early exit, took #{elapsed}s"
    end

    test "interruptible_sleep no-ops with non-positive total" do
      TickLoop.interruptible_sleep(0, -> { false })
      TickLoop.interruptible_sleep(-1, -> { false })
      # No assertion — just making sure it doesn't raise or hang.
      assert true
    end
  end
end
