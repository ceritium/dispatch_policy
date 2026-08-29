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
end
