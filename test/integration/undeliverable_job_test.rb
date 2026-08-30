# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# `Forwarder.dispatch` deserializes every row of a batch before enqueuing
# any of them, inside the admission transaction. A row whose job_class no
# longer resolves — a deploy renamed it, dropped it, or moved it into a
# component the tick process does not load — therefore rolls the whole
# batch back. That rollback is correct: it is the at-least-once
# guarantee. What was not correct is what came next: the claim orders by
# priority then id, so the same poisoned row heads every subsequent
# batch, forever, and the healthy rows behind it in that partition are
# never admitted again. Nothing else deletes from staged_jobs, there is
# no staged retention sweep, and the sweeper keeps the partition because
# it still has rows — so the only exit was hand-written SQL.
class UndeliverableJobTest < DispatchPolicy::IntegrationTest
  class LiveJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("undeliverable") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(LiveJob::POLICY, owner: LiveJob.name)
  end

  def stage!(job_class)
    DispatchPolicy::Repository.stage!(
      policy_name: "undeliverable", partition_key: "k", queue_name: nil,
      job_class: job_class,
      job_data: LiveJob.new.serialize.merge("job_class" => job_class),
      context: {}
    )
  end

  def partition
    DispatchPolicy::Partition.find_by(policy_name: "undeliverable", partition_key: "k")
  end

  def test_a_poisoned_row_is_quarantined_and_its_neighbours_still_go_out
    stage!("VanishedJob")   # sorts first: same priority, lowest id
    stage!(LiveJob.name)
    stage!(LiveJob.name)

    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal 0, DispatchPolicy::StagedJob.deliverable.count,
                 "the healthy rows behind the poison must be admitted on the same tick"
    quarantined = DispatchPolicy::StagedJob.quarantined
    assert_equal 1, quarantined.count
    assert_equal "VanishedJob", quarantined.first.job_class
    assert_includes quarantined.first.failure_reason, "NameError"
  end

  def test_a_quarantined_row_stops_counting_as_pending_work
    stage!("VanishedJob")
    stage!(LiveJob.name)
    assert_equal 2, partition.pending_count

    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal 0, partition.pending_count,
                 "a row nothing will ever admit is not a backlog; leaving it counted " \
                 "keeps claim_partitions returning the partition forever"
  end

  def test_the_next_tick_does_not_retry_it
    stage!("VanishedJob")
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    failed_at = DispatchPolicy::StagedJob.quarantined.first.failed_at

    stage!(LiveJob.name)
    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal failed_at, DispatchPolicy::StagedJob.quarantined.first.failed_at,
                 "the claim must skip it, not re-attempt and re-mark it every tick"
    assert_equal 0, DispatchPolicy::StagedJob.deliverable.count
  end

  # The drain button is what an operator reaches for when a partition
  # looks stuck, so it has to get past the same row.
  def test_a_forced_admission_gets_past_it_too
    stage!("VanishedJob")
    stage!(LiveJob.name)

    forwarded = DispatchPolicy::ManualAdmission.force!(
      policy_name: "undeliverable", partition_key: "k", limit: 10
    )

    assert_equal 1, forwarded
    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count
  end
end
