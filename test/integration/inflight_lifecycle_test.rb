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

  # Bound to a policy WITHOUT the `dispatch_policy` macro — the class only
  # names one. This is public API (dispatch_policy_name is a public
  # class_attribute), it is the only way to point two classes at one
  # policy since a second macro call raises PolicyAlreadyRegistered, and
  # it is what several of this suite's own cases do. Admission keys off
  # the POLICY, so if release keyed off the macro instead, these jobs
  # would be admitted, counted, and never released.
  class NoMacroJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    SHARED_POLICY = DispatchPolicy::PolicyDSL.build("inflight_lifecycle_no_macro") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :concurrency, max: 2, full_backoff: 0
    end

    self.dispatch_policy_name = "inflight_lifecycle_no_macro"

    def perform(*); end
  end

  # The macro BEFORE the policy block — the order the dummy app's jobs use.
  class MacroFirstJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    dispatch_policy_inflight_tracking

    POLICY = dispatch_policy("inflight_lifecycle_macro_first") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :adaptive_concurrency, initial_max: 5, target_lag_ms: 1000, min: 1
    end

    def perform(*); end
  end

  # …and AFTER it, which is the order that would nest two `track` wrappers
  # if including the module twice installed two callbacks.
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

  # A gate-less policy that opts in by hand: nothing needs these rows for
  # admission, but the operator wants a live in-flight count. Records what
  # the table looked like MID-perform, which is the only moment the row is
  # supposed to exist.
  class OptedInJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    dispatch_policy_inflight_tracking

    POLICY = dispatch_policy("inflight_lifecycle_opt_in") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 100, per: 60
    end

    class << self
      attr_accessor :rows_during_perform
    end

    def perform(*)
      self.class.rows_during_perform = DispatchPolicy::InflightJob.count
    end
  end

  ALL_JOB_CLASSES = [
    ForgotTheMacroJob, ThrottleOnlyJob, MacroFirstJob, MacroLastJob, OptedInJob
  ].freeze

  def setup
    super

    # The class bodies above registered their policies at load time;
    # reset_dispatch_policy! wipes the registry before every test, so put
    # them back without re-running the macro (which would stack another
    # around_enqueue callback per test).
    ALL_JOB_CLASSES.each do |klass|
      DispatchPolicy.registry.register(klass::POLICY, owner: klass.name)
    end
    DispatchPolicy.registry.register(NoMacroJob::SHARED_POLICY, owner: NoMacroJob.name)

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

    # Forwarder sends immediate rows through ActiveJob.perform_all_later,
    # which uses the adapter's enqueue_all when it has one and falls back
    # to per-job enqueue when it doesn't. Today's TestAdapter has no
    # enqueue_all, so capturing :enqueue alone happens to work — and would
    # silently stop working, leaving `received` empty and the assertions
    # below measuring nothing, the day ActiveJob gives it one.
    captures_bulk = adapter.method_defined?(:enqueue_all) || adapter.private_method_defined?(:enqueue_all)
    if captures_bulk
      adapter.alias_method(:__orig_enqueue_all, :enqueue_all)
      adapter.define_method(:enqueue_all) do |jobs|
        received.concat(jobs)
        __orig_enqueue_all(jobs)
      end
    end

    yield received
  ensure
    adapter.alias_method(:enqueue, :__orig_enqueue)
    adapter.alias_method(:enqueue_all, :__orig_enqueue_all) if captures_bulk
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

  # The hole the first version of this fix left open: it installed the
  # release callback from inside the `dispatch_policy` macro, while
  # admission decided from the registered policy. A class that only names
  # its policy got rows created and never released — the original wedge,
  # through supported plumbing.
  def test_a_class_bound_without_the_macro_still_releases_its_rows
    # perform_all_later is the entry point that stages a macro-less class:
    # BulkEnqueue.stageable? asks only for a registered policy name, so
    # these jobs are admitted exactly like any other. (The single-job
    # around_enqueue is installed by the macro, so `perform_later` alone
    # would hand them straight to the adapter.)
    unless ActiveJob.singleton_class.include?(DispatchPolicy::JobExtension::BulkEnqueue)
      ActiveJob.singleton_class.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
    end

    capturing_adapter_enqueues do |received|
      ActiveJob.perform_all_later(NoMacroJob.new, NoMacroJob.new, NoMacroJob.new)
      assert_equal 3, DispatchPolicy::StagedJob.count, "the bulk path must stage them"

      DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_no_macro")

      assert_equal 2, received.size, "concurrency max: 2 admits two jobs"
      assert_equal 2, DispatchPolicy::InflightJob.count,
                   "admission pre-inserts from the POLICY, macro or no macro"

      received.each(&:perform_now)

      assert_equal 0, DispatchPolicy::InflightJob.count,
                   "performing must release the rows; if it doesn't, the partition is wedged " \
                   "at max until the 1h sweeper — the H3 bug, reachable without the macro"

      DispatchPolicy::Tick.run(policy_name: "inflight_lifecycle_no_macro")
      assert_equal 3, received.size, "the freed slots must let the last job through"
    end
  end

  # A gate-less policy that opts in by hand still gets tracked: the class
  # attribute ADDS tracking, it never gates it.
  def test_the_manual_opt_in_tracks_a_gate_less_policy
    OptedInJob.rows_during_perform = nil
    OptedInJob.new.perform_now

    assert_equal 1, OptedInJob.rows_during_perform,
                 "the opt-in must produce a row for the dashboard to count while the job runs"
    assert_equal 0, DispatchPolicy::InflightJob.count,
                 "and release it when the job finishes"
  end

  # A worker whose registry no longer has the policy (renamed or removed
  # while an older tick was still admitting) must still release the row a
  # tick pre-inserted, rather than leave it holding a slot for an hour.
  def test_a_job_whose_policy_vanished_still_releases_its_row
    job = ForgotTheMacroJob.new
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name:   "inflight_lifecycle_conc",
      partition_key: "k",
      active_job_id: job.job_id
    }])
    assert_equal 1, DispatchPolicy::InflightJob.count

    DispatchPolicy.registry.clear

    job.perform_now

    assert_equal 0, DispatchPolicy::InflightJob.count,
                 "an unknown policy is no reason to strand a row keyed on active_job_id alone"
  end
end
