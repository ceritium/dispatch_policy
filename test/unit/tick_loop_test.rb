# frozen_string_literal: true

require_relative "../test_helper"

class TickLoopTest < Minitest::Test
  # A non-positive pause must be a no-op, not sleep(-x) → ArgumentError that
  # escapes the loop's rescues and kills the tick chain.
  def test_pause_is_a_noop_for_non_positive_values
    DispatchPolicy::TickLoop.pause(0)
    DispatchPolicy::TickLoop.pause(-1)
    DispatchPolicy::TickLoop.pause(nil)
    pass
  end

  # The loop body computes `iteration % sweep_every_ticks`; a 0 there used to
  # raise ZeroDivisionError OUTSIDE the per-tick rescue, killing the loop.
  # It now treats <= 0 as "never sweep". A registered policy makes `names`
  # non-empty so the loop reaches the modulo line (Tick.run failing without
  # a DB is rescued, which is fine — we only care that the loop survives).
  def test_sweep_every_ticks_zero_does_not_crash_and_never_sweeps
    DispatchPolicy.reset_registry!
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build("p") do
        partition_by ->(_c) { "k" }
        gate :throttle, rate: 1, per: 60
      end
    )

    swept = false
    DispatchPolicy::TickLoop.singleton_class.alias_method(:__orig_sweep, :sweep!)
    DispatchPolicy::TickLoop.singleton_class.define_method(:sweep!) { swept = true }

    DispatchPolicy.config.sweep_every_ticks = 0
    DispatchPolicy.config.idle_pause        = 0
    DispatchPolicy.config.busy_pause        = 0

    iterations = 0
    # Must not raise ZeroDivisionError; loop exits via stop_when.
    DispatchPolicy::TickLoop.run(stop_when: -> { (iterations += 1) > 3 })

    refute swept, "sweep_every_ticks = 0 must mean never sweep"
  ensure
    DispatchPolicy::TickLoop.singleton_class.alias_method(:sweep!, :__orig_sweep)
    DispatchPolicy.reset_config!
    DispatchPolicy.reset_registry!
  end
end
