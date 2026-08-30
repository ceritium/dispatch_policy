# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# ActiveJob 7.2+ lets a job class defer its own enqueue past the
# surrounding transaction — the setting Rails recommends for apps that
# enqueue inside AR transactions. The forward runs INSIDE the admission
# TX, so that deferral registers the real enqueue on the gem's own
# transaction: it lands after COMMIT, outside the Bypass window, and the
# job the tick just admitted is staged all over again (or the admission
# rolls back). Either way it never reaches the adapter, on every tick,
# forever, with nothing in the logs.
#
# A full Rails app includes this module into ActiveJob::Base; the gem's
# test environment wires ActiveJob by hand, so mirror it here. Harmless
# for the rest of the suite: with the flag at its default `false` the
# prepended methods just call super.
ActiveJob::Base.include(ActiveJob::EnqueueAfterTransactionCommit)

class DeferredEnqueueTest < DispatchPolicy::IntegrationTest
  class DeferredJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    self.enqueue_after_transaction_commit = true

    POLICY = dispatch_policy("deferred_enqueue") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :concurrency, max: 100
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(DeferredJob::POLICY, owner: DeferredJob.name)
    ActiveJob::Base.queue_adapter.enqueued_jobs.clear
  end

  def adapter_jobs
    ActiveJob::Base.queue_adapter.enqueued_jobs
  end

  def test_an_immediate_job_reaches_the_adapter
    DeferredJob.perform_later
    adapter_jobs.clear # the staging call itself never touches the adapter

    DispatchPolicy::Tick.run(policy_name: "deferred_enqueue")

    assert_equal 0, DispatchPolicy::StagedJob.count,
                 "the admission must not roll back: the deferred enqueue has to happen inside it"
    assert_equal 1, adapter_jobs.size, "the job has to actually reach the adapter"
  end

  def test_a_due_scheduled_job_is_not_re_staged
    DispatchPolicy::Repository.stage!(
      policy_name:   "deferred_enqueue",
      partition_key: "k",
      queue_name:    nil,
      job_class:     DeferredJob.name,
      job_data:      DeferredJob.new.serialize,
      context:       {},
      scheduled_at:  1.minute.ago
    )
    adapter_jobs.clear

    3.times { DispatchPolicy::Tick.run(policy_name: "deferred_enqueue") }

    assert_equal 0, DispatchPolicy::StagedJob.count,
                 "a job re-staged by its own deferred enqueue never drains"
    assert_equal 1, adapter_jobs.size
    assert_equal 1, DispatchPolicy::InflightJob.count,
                   "one admission, one inflight row — the re-stage loop leaks one per tick"
  end
  # `transaction` swallows ActiveRecord::Rollback by design, so the
  # savepoint the deferral fix introduces would absorb one raised anywhere
  # in the forward: dispatch returns normally, the Tick counts the
  # admission, and the transaction commits with the staged rows deleted,
  # the inflight rows inserted, and nothing in the adapter. Without the
  # savepoint that same exception reaches admit_partition's transaction
  # and aborts the admission — the two paths have to agree.
  def test_a_rollback_inside_the_savepoint_still_aborts_the_admission
    DeferredJob.perform_later
    adapter_jobs.clear

    boom = ->(*) { raise ActiveRecord::Rollback }
    DispatchPolicy::Bypass.stub(:with, boom) do
      DispatchPolicy::Tick.run(policy_name: "deferred_enqueue")
    end

    assert_equal 1, DispatchPolicy::StagedJob.count,
                 "the staged row must survive; committing here loses the job silently"
    assert_equal 0, adapter_jobs.size
    assert_equal 0, DispatchPolicy::InflightJob.count,
                   "and the pre-inserted inflight row goes back with it"
  end
end
