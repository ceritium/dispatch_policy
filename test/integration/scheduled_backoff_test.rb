# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# M10: a partition holding nothing but future-scheduled work used to be
# re-claimed on every single tick. `claim_partitions` selects on
# pending_count (which counts scheduled rows) while `claim_staged_jobs!`
# only takes rows that are due, so the tick claimed it, found nothing,
# left next_eligible_at NULL, and did it all again a moment later —
# burning a partition_batch_size slot and a transaction each time, and
# filling the denial breakdown with no_rows_claimed.
#
# The horizon lives in `scheduled_eligible_at`, NOT in `next_eligible_at`.
# The two answer different questions — "is there anything to do yet?"
# versus "did a gate tell us to wait?" — and sharing one column meant a
# job enqueued for right now could not clear the wait without also
# clobbering a gate's backoff, so it sat unadmitted until the far-future
# horizon arrived.
class ScheduledBackoffTest < DispatchPolicy::IntegrationTest
  class ScheduledJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("scheduled_backoff") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 100, per: 60
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(ScheduledJob::POLICY, owner: ScheduledJob.name)
  end

  def partition
    DispatchPolicy::Partition.first
  end

  def test_a_partition_with_only_future_work_parks_until_it_is_due
    ScheduledJob.set(wait: 1.hour).perform_later
    assert_equal 1, DispatchPolicy::StagedJob.count

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    due_at = DispatchPolicy::StagedJob.first.scheduled_at
    refute_nil partition.scheduled_eligible_at,
               "a NULL horizon makes the partition eligible again immediately"
    assert_in_delta due_at.to_f, partition.scheduled_eligible_at.to_f, 1.0,
                    "it should wake exactly when the job becomes due"
    assert_nil partition.next_eligible_at,
               "no gate has asked for a backoff; that column is not ours to write"
  end

  def test_the_parked_partition_is_not_reclaimed_on_the_next_tick
    ScheduledJob.set(wait: 1.hour).perform_later

    3.times { DispatchPolicy::Tick.run(policy_name: "scheduled_backoff") }

    seen = DispatchPolicy::TickSample.order(:id).pluck(:partitions_seen)
    assert_equal [0, 0, 0], seen,
                 "the horizon is set by the enqueue itself, so not even the first " \
                 "tick spends a claim slot on work that is not due"
  end

  # The soonest job is what matters — parking until the last one would
  # hold back work that becomes due before it.
  def test_it_parks_until_the_earliest_scheduled_job
    ScheduledJob.set(wait: 2.hours).perform_later
    ScheduledJob.set(wait: 10.minutes).perform_later

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    earliest = DispatchPolicy::StagedJob.minimum(:scheduled_at)
    assert_in_delta earliest.to_f, partition.scheduled_eligible_at.to_f, 1.0
  end

  # A gate that just asked for a backoff is talking about capacity, which
  # outranks "when is the next job due".
  def test_it_does_not_overwrite_a_backoff_a_gate_just_set
    ScheduledJob.set(wait: 1.hour).perform_later
    DispatchPolicy::Repository.bulk_record_partition_denies!([{
      policy_name:      "scheduled_backoff",
      partition_key:    "k",
      gate_state_patch: {},
      retry_after:      30
    }])
    gate_backoff = partition.next_eligible_at

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    assert_in_delta gate_backoff.to_f, partition.next_eligible_at.to_f, 0.001,
                    "the gate's backoff must survive"
    refute_nil partition.scheduled_eligible_at,
               "and the scheduled horizon is tracked alongside it, not instead of it"
  end

  # Due work must still be admitted normally — the park only applies when
  # the claim came back empty.
  def test_due_work_is_unaffected
    ScheduledJob.perform_later
    ScheduledJob.set(wait: 1.hour).perform_later

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    assert_equal 1, DispatchPolicy::StagedJob.count, "the due job is admitted"
    assert_nil partition.next_eligible_at,
               "an admitting tick must not park the partition"
    assert_nil partition.scheduled_eligible_at,
               "work that is due now means there is no horizon to wait for"
  end

  # The bug the separate column exists for. A partition parked on a
  # far-future job used to be invisible to `claim_partitions` until that
  # horizon arrived, so a job enqueued for RIGHT NOW sat in staged_jobs
  # for the whole wait — an hour here, a week with `wait: 1.week` — with
  # no gate having denied it and nothing in the logs. Overwriting
  # `next_eligible_at` from the enqueue path is not the fix either: that
  # is where gates keep their backoff, and clearing it on every enqueue
  # brings back the busy-loop the backoff exists to prevent.
  def test_a_job_due_now_wakes_a_partition_parked_on_a_future_one
    ScheduledJob.set(wait: 1.hour).perform_later
    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")
    refute_nil partition.scheduled_eligible_at, "parked on the future job"

    ScheduledJob.perform_later # due now

    assert_nil partition.scheduled_eligible_at,
               "due work has to clear the horizon or it will never be claimed"

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    assert_equal 1, DispatchPolicy::StagedJob.count,
                 "the due job goes out on the next tick, not in an hour"
    assert_equal 1, DispatchPolicy::TickSample.where("partitions_seen > 0").count
  end

  # ...and the reverse must not happen: a future job arriving cannot
  # install a horizon over a partition that already has due work waiting,
  # or one `wait: 1.week` call would strand everything behind it.
  def test_a_future_job_does_not_park_a_partition_that_has_due_work
    ScheduledJob.perform_later
    ScheduledJob.set(wait: 1.week).perform_later

    assert_nil partition.scheduled_eligible_at

    DispatchPolicy::Tick.run(policy_name: "scheduled_backoff")

    assert_equal 1, DispatchPolicy::StagedJob.count, "the due job left"
  end
end
