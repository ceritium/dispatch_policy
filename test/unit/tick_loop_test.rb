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
  # `stop_when` is host-supplied — the generated tick job asks the adapter
  # whether it is shutting down — and it is called outside the rescue that
  # guards Tick.run. A raise there escapes `perform`, so the job's
  # self-re-enqueue never happens: admission stops for good while
  # perform_later keeps filling a staging table nothing empties. That is
  # how a single wrong method name in the generated job (solid_queue never
  # had SolidQueue::Process.current_process) took down a whole install.
  def test_a_raising_stop_when_does_not_kill_the_loop
    DispatchPolicy.reset_registry!
    calls = 0
    stop_when = lambda do
      calls += 1
      raise NoMethodError, "undefined method 'current_process'" if calls == 1

      calls >= 3
    end

    DispatchPolicy.config.idle_pause = 0
    DispatchPolicy::TickLoop.run(stop_when: stop_when)

    assert_operator calls, :>=, 3, "the loop kept going and stopped when actually told to"
  ensure
    DispatchPolicy.reset_config!
  end

  # The generated job must only call adapter methods that exist. Both
  # supported adapters implement the ActiveJob `stopping?` hook; the
  # solid_queue branch used to call a method solid_queue has never had, so
  # every install died on its first iteration.
  def test_the_generated_tick_job_only_uses_the_activejob_shutdown_hook
    template = File.read(
      File.expand_path(
        "../../lib/generators/dispatch_policy/install/templates/dispatch_tick_loop_job.rb.tt", __dir__
      )
    )
    body = template[/def adapter_shutting_down\?(.*?)\n  end/m, 1]

    refute_nil body
    refute_includes body, "current_process"
    assert_includes body, "stopping?"
    assert_includes body, "respond_to?"
  end
end
