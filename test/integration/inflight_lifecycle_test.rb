# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/adaptive_concurrency_stats"

# H3 — the two ends of the inflight row lifecycle must always agree.
#
# The Tick pre-inserts a row in dispatch_policy_inflight_jobs per admitted
# job so a concurrency-family gate counts it immediately; the ONLY thing
# that removes it is InflightTracker.track's ensure. Before the fix,
# creation was unconditional while deletion was an opt-in macro the job
# class had to remember:
#
#   - concurrency gate + a class that forgot the macro → the partition
#     wedged at `max` until the inflight_queued_stale_after sweeper (1h)
#     reaped the rows, with no error anywhere;
#   - no concurrency gate (where the README says the macro isn't needed)
#     → one orphan row per admitted job for an hour, inflating the
#     dashboard's in-flight count with jobs that had long finished.
#
# Now `dispatch_policy` installs the callback for concurrency-family
# policies, and the Tick only pre-inserts for those same policies.
class InflightLifecycleTest < DispatchPolicy::IntegrationTest
  # Deliberately does NOT declare dispatch_policy_inflight_tracking: the
  # whole point is that a host that forgets it is still correct.
  class ForgotTheMacroJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("inflight_lifecycle_conc") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      # full_backoff: 0 so a "gate full" deny doesn't park the partition
      # for a second — this test runs its ticks back to back and is about
      # the inflight rows, not about backoff timing.
      gate :concurrency, max: 2, full_backoff: 0
    end

    def perform(*); end
  end

  class ThrottleOnlyJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("inflight_lifecycle_thr") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 100, per: 60
    end

    def perform(*); end
  end

  # The macro BEFORE the policy block — the order the dummy app's jobs use.
  class MacroFirstJob < ActiveJob::Base
    include DispatchPolicy::JobExtension
    include DispatchPolicy::InflightTracker

    dispatch_policy_inflight_tracking

    POLICY = dispatch_policy("inflight_lifecycle_macro_first") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :adaptive_concurrency, initial_max: 5, target_lag_ms: 1000, min: 1
    end

    def perform(*); end
  end

  # …and AFTER it, which is the order that would nest two `track` wrappers
  # if the macro weren't idempotent.
  class MacroLastJob < ActiveJob::Base
    include DispatchPolicy::JobExtension
    include DispatchPolicy::InflightTracker

    POLICY = dispatch_policy("inflight_lifecycle_macro_last") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :adaptive_concurrency, initial_max: 5, target_lag_ms: 1000, min: 1
    end

    dispatch_policy_inflight_tracking

    def perform(*); end
  end

  def setup
    super

    # The class bodies above registered their policies at load time;
    # reset_dispatch_policy! wipes the registry before every test, so put
    # them back without re-running the macro (which would stack another
    # around_enqueue callback per test).
    [ForgotTheMacroJob, ThrottleOnlyJob, MacroFirstJob, MacroLastJob].each do |klass|
      DispatchPolicy.registry.register(klass::POLICY, owner: klass.name)
    end

    # No heartbeat thread: these tests are about insert/delete, and a
    # thread per perform only makes them slower and flakier.
    DispatchPolicy.config.inflight_heartbeat_interval = 0
  end

  def teardown
    DispatchPolicy.reset_registry!
  end

  # Captures the job instances the adapter receives, so the test can run
  # their perform callbacks exactly as a worker would — with the
  # active_job_id the Tick regenerated at admission.
  def capturing_adapter_enqueues
    received = []
    adapter  = ActiveJob::Base.queue_adapter.singleton_class
    adapter.alias_method(:__orig_enqueue, :enqueue)
    adapter.define_method(:enqueue) do |job|
      received << job
      __orig_enqueue(job)
    end
    yield received
  ensure
    adapter.alias_method(:enqueue, :__orig_enqueue)
  end

  def test_concurrency_policy_drains_even_when_the_job_class_forgot_the_macro
    capturing_adapter_enqueues do |received|
      5.times { ForgotTheMacroJob.perform_later }
      assert_equal 5, DispatchPolicy::StagedJob.count

      DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_conc")
      assert_equal 2, received.size, "concurrency max: 2 admits two jobs"
      assert_equal 2, DispatchPolicy::InflightJob.count,
                   "admission must pre-insert so the gate counts them"

      # The gate is full: further ticks admit nothing until the jobs run.
      DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_conc")
      assert_equal 2, received.size

      # A worker performs them. The auto-installed around_perform releases
      # the rows; without it the partition stayed wedged for an hour.
      received.each(&:perform_now)
      performed = received.size
      assert_equal 0, DispatchPolicy::InflightJob.count,
                   "perform must release the inflight rows"

      3.times do
        DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_conc")
        received[performed..].each(&:perform_now)
        performed = received.size
      end

      assert_equal 0, DispatchPolicy::StagedJob.count,
                   "the whole backlog must drain; if this fails the partition is wedged again"
      assert_equal 5, received.size
    end
  end

  def test_policy_without_a_concurrency_gate_creates_no_inflight_rows
    ThrottleOnlyJob.perform_later
    DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_thr")

    assert_equal 0, DispatchPolicy::StagedJob.count, "the job must be admitted"
    assert_equal 0, DispatchPolicy::InflightJob.count,
                   "nothing reads this table for a gate-less policy, and nothing would " \
                   "delete the row until the 1h sweeper"
  end

  def test_manual_admission_follows_the_same_rule
    ThrottleOnlyJob.perform_later
    forwarded = DispatchPolicy::ManualAdmission.force!(
      policy_name: "inflight_lifecycle_thr", partition_key: "k", limit: 10
    )

    assert_equal 1, forwarded
    assert_equal 0, DispatchPolicy::InflightJob.count
  end

  def test_declaring_the_macro_by_hand_still_tracks_exactly_once
    { MacroFirstJob => "inflight_lifecycle_macro_first",
      MacroLastJob  => "inflight_lifecycle_macro_last" }.each do |klass, policy_name|
      klass.new.perform_now

      stats = DispatchPolicy::AdaptiveConcurrencyStats.find_by(
        policy_name: policy_name, partition_key: "k"
      )
      refute_nil stats, "#{klass.name}: the perform must record an adaptive observation"
      assert_equal 1, stats.sample_count,
                   "#{klass.name}: two nested track wrappers would record two observations " \
                   "per perform and delete the inflight row from inside the inner ensure"
      assert_equal 0, DispatchPolicy::InflightJob.where(policy_name: policy_name).count,
                   "#{klass.name}: the row must be released once"
    end
  end

  def test_auto_install_marks_the_class_and_is_not_repeated
    assert ForgotTheMacroJob.dispatch_policy_inflight_tracking_installed,
           "a concurrency policy must install the tracking callback on its job class"
    refute ThrottleOnlyJob.dispatch_policy_inflight_tracking_installed,
           "a gate-less policy must not pay for tracking it doesn't need"
  end
end
