# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# The generated DispatchTickLoopJob calls TickLoop.run for one bounded
# window and re-enqueues itself. A per-invocation counter therefore
# restarts every window, and with the shipped defaults a window holds at
# most tick_max_duration / idle_pause = 25 / 0.5 = 50 iterations —
# exactly sweep_every_ticks. Any per-iteration cost leaves the count at
# 49 and the sweep never fires at all.
#
# The consequence is worst on the partitions that need it most: a stale
# inflight row makes a concurrency gate deny, denying means admitting 0,
# admitting 0 means idle_pause, and idle_pause is what keeps the window
# under 50 iterations. The wedge feeds itself.
class SweepCadenceTest < DispatchPolicy::IntegrationTest
  class CadenceJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("sweep_cadence") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :concurrency, max: 1
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(CadenceJob::POLICY, owner: CadenceJob.name)
    DispatchPolicy::TickLoop.reset_sweep_cadence!
    DispatchPolicy.config.sweep_every_ticks = 3
    DispatchPolicy.config.idle_pause = 0
    DispatchPolicy.config.busy_pause = 0
  end

  def teardown
    DispatchPolicy.reset_config!
    DispatchPolicy::TickLoop.reset_sweep_cadence!
  end

  # One iteration per call, the way the tick job's bounded window does it.
  def run_one_iteration!
    base = DispatchPolicy::TickSample.count
    DispatchPolicy::TickLoop.run(
      policy_name: "sweep_cadence",
      stop_when:   -> { DispatchPolicy::TickSample.count > base }
    )
  end

  def test_the_sweep_cadence_survives_the_tick_job_re_enqueuing_itself
    # A worker was SIGKILLed holding this admission: the row is past
    # inflight_stale_after with a heartbeat that stopped advancing, which
    # is exactly what the tier-1 sweep exists to reap.
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "sweep_cadence", partition_key: "k", active_job_id: "ajid-dead"
    }])
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_inflight_jobs
      SET admitted_at  = now() - interval '20 minutes',
          heartbeat_at = now() - interval '10 minutes'
    SQL

    2.times { run_one_iteration! }
    assert_equal 1, DispatchPolicy::InflightJob.count,
                 "two iterations is short of the cadence; nothing should have been swept yet"

    run_one_iteration!

    assert_equal 0, DispatchPolicy::InflightJob.count,
                 "three iterations is the cadence — counted across invocations, not within one"
  end
end
