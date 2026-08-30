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
  # The claim gained `failed_at IS NULL`; the scheduled park did not, and
  # its "is anything due?" guard counts a quarantined row with a NULL
  # scheduled_at as due work. So the park never fires and the partition is
  # re-claimed, re-evaluated and denied on every single tick — the M10/M13
  # busy loop, permanently, on a partition that will never admit anything.
  def test_a_quarantined_row_does_not_keep_the_partition_from_parking
    stage!("VanishedJob")
    DispatchPolicy::Repository.stage!(
      policy_name: "undeliverable", partition_key: "k", queue_name: nil,
      job_class: LiveJob.name, job_data: LiveJob.new.serialize, context: {},
      scheduled_at: 1.hour.from_now
    )

    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count
    refute_nil partition.scheduled_eligible_at,
               "the only deliverable row is an hour out; leaving the horizon NULL " \
               "re-claims this partition on every tick forever"
  end

  # The quarantine needs a working inverse. Clearing `failed_at` by hand —
  # which the UI, the CHANGELOG and CLAUDE.md all used to prescribe — is
  # not one: the quarantine decremented pending_count, `claim_partitions`
  # requires `pending_count > 0`, so the row comes back deliverable and no
  # tick ever claims it again.
  def test_requeue_puts_a_quarantined_row_back_in_play
    stage!("VanishedJob")
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count
    assert_equal 0, partition.pending_count

    # The deploy that brings the class back.
    Object.const_set(:VanishedJob, Class.new(LiveJob))
    begin
      requeued = DispatchPolicy::Repository.requeue_quarantined_jobs!(
        policy_name: "undeliverable", partition_key: "k"
      )
      assert_equal 1, requeued
      assert_equal 1, partition.pending_count,
                   "without the counter the row is deliverable and unclaimable at once"

      DispatchPolicy::Tick.run(policy_name: "undeliverable")
      assert_equal 0, DispatchPolicy::StagedJob.count, "and now it actually goes out"
    ensure
      Object.send(:remove_const, :VanishedJob)
    end
  end

  # The partition sweeper deletes on `pending_count = 0`, which before the
  # quarantine could not happen while rows existed. Now it can — and
  # collecting the partition would orphan the quarantined rows in the
  # gem's most write-hot table, with nothing left pointing at them.
  def test_the_sweeper_keeps_a_partition_that_still_holds_quarantined_rows
    stage!("VanishedJob")
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    assert_equal 0, partition.pending_count

    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_partitions
      SET last_admit_at = now() - interval '48 hours',
          created_at    = now() - interval '48 hours'
    SQL
    DispatchPolicy::TickLoop.sweep!

    refute_nil partition,
               "deleting it strands the quarantined rows with no route back to them"
  end
end
