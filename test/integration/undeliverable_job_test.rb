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
  # The premise "a class that will not resolve is never coming back" is
  # false for the most ordinary deploy there is: ADDING a job class. Web
  # pods roll first and stage jobs the tick pod's image cannot resolve
  # yet, so it holds them back — and if the hold is terminal, finishing
  # the rollout does not release them. That turns a visible, self-healing
  # stall into a silent permanent drop, which is the at-least-once
  # failure the admission TX exists to prevent.
  def test_a_hold_expires_so_a_finished_rollout_releases_the_rows
    stage!("NotYetDeployedJob")
    stage!("NotYetDeployedJob")
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    assert_equal 2, DispatchPolicy::StagedJob.quarantined.count

    # The rollout finishes: the class resolves everywhere now.
    Object.const_set(:NotYetDeployedJob, Class.new(LiveJob))
    begin
      # Age the hold past the retry window and let the sweeper run.
      ActiveRecord::Base.connection.execute(
        "UPDATE dispatch_policy_staged_jobs SET failed_at = now() - interval '2 hours'"
      )
      DispatchPolicy::TickLoop.sweep!

      assert_equal 0, DispatchPolicy::StagedJob.quarantined.count,
                   "a hold that never expires drops the class's whole backlog"
      assert_equal 2, partition.pending_count

      DispatchPolicy::Tick.run(policy_name: "undeliverable")
      assert_equal 0, DispatchPolicy::StagedJob.count, "and they actually go out"
    ensure
      Object.send(:remove_const, :NotYetDeployedJob)
    end
  end

  def test_a_class_that_is_really_gone_is_simply_held_again
    stage!("VanishedJob")
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_staged_jobs SET failed_at = now() - interval '2 hours'"
    )

    DispatchPolicy::TickLoop.sweep!
    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count,
                 "releasing the hold must not lose the row either"
  end

  # The Serializer wraps ONLY the constantize, so the log can name the
  # ordinary missing-class case. That split is about the error class, not
  # about what gets held: the Forwarder holds the row for any deserialize
  # failure — see the two tests below.
  def test_the_serializer_only_wraps_the_class_lookup
    err = assert_raises(NoMethodError) do
      DispatchPolicy::Serializer.deserialize(
        { "job_class" => LiveJob.name, "arguments" => nil, "boom" => true }.tap do |p|
          def p.[](k) = k == "job_class" ? "UndeliverableJobTest::LiveJob" : nil.no_such_method
        end
      )
    end
    refute_kind_of DispatchPolicy::UnresolvableJobClass, err
  end
  # The hold has to cover any deserialize failure, not just the
  # constantize. Anything else escapes to the Tick's generic rescue,
  # which only queues a backoff — no `failed_at`, so nothing ever
  # releases it, and the row heads every claim of that partition forever.
  # That is the wedge the hold exists to prevent, reopened from the other
  # side.
  def test_a_deserialize_failure_that_is_not_a_missing_constant_is_also_held
    DispatchPolicy::Repository.stage!(
      policy_name: "undeliverable", partition_key: "k", queue_name: nil,
      job_class: "String", # resolves fine; String.deserialize does not exist
      job_data: { "job_class" => "String" }, context: {}
    )
    stage!(LiveJob.name)

    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count,
                 "not held means not released either — it wedges the partition forever"
    assert_equal 0, DispatchPolicy::StagedJob.deliverable.count,
                 "and the healthy row behind it still goes out"
  end
  # The dashboard's "Staged" tile is what an operator reads as backlog.
  # Counting held rows there says work is in motion when nothing is
  # trying to admit them, and feeds a drain-time estimate that can never
  # come true — so they get their own tile instead.
  def test_held_rows_are_not_counted_as_staged_backlog
    stage!("VanishedJob")
    stage!(LiveJob.name)
    DispatchPolicy::Tick.run(policy_name: "undeliverable")
    stage!(LiveJob.name)

    assert_equal 1, DispatchPolicy::StagedJob.quarantined.count
    assert_equal 1, DispatchPolicy::StagedJob.deliverable.count

    source = File.read(File.expand_path(
      "../../app/controllers/dispatch_policy/dashboard_controller.rb", __dir__
    ))
    assert_match(/staged:\s+StagedJob\.deliverable\.count/, source,
                 "counting held rows as staged is what made the tile lie")
    assert_match(/quarantined:\s+StagedJob\.quarantined\.count/, source)
  end
  # `String.deserialize` raises NoMethodError, which IS a NameError — so a
  # rescue listing NameError catches it and the test above passes either
  # way. Most of what `klass.deserialize` can raise is not a NameError at
  # all, and stock ActiveJob supplies the cheapest example: a
  # `scheduled_at` stored as a Float (what Rails <= 7.1 wrote) makes
  # `deserialize_time` raise TypeError. Uncaught, that escapes to the
  # Tick's generic rescue, which queues a backoff and writes no
  # `failed_at` — so nothing releases the row and it heads every claim of
  # that partition forever, healthy neighbours included. That is the
  # wedge, reopened from the far side.
  def test_a_deserialize_failure_that_is_not_a_name_error_is_held_too
    DispatchPolicy::Repository.stage!(
      policy_name: "undeliverable", partition_key: "k", queue_name: nil,
      job_class: LiveJob.name,
      job_data: LiveJob.new.serialize.merge("scheduled_at" => Time.now.to_f),
      context: {}
    )
    stage!(LiveJob.name)

    DispatchPolicy::Tick.run(policy_name: "undeliverable")

    held = DispatchPolicy::StagedJob.quarantined
    assert_equal 1, held.count,
                 "not held means not released either — it wedges the partition forever"
    refute_match(/NameError/, held.first.failure_reason,
                 "if this is a NameError the test proves nothing the sibling did not")
    assert_equal 0, DispatchPolicy::StagedJob.deliverable.count,
                 "and the healthy row behind it still goes out"
  end
  # The release runs FIRST inside `sweep_inactive_partitions!`, so without
  # its own rescue anything it raises — a deadlock against `stage_many!`
  # is the realistic one — takes the whole sweep with it: no partition GC,
  # no tick-sample retention, no adaptive-stat GC. A condition that does
  # not clear itself does that on every sweep for the life of the process,
  # silently, because `sweep!`'s outer rescue only logs.
  def test_a_failed_quarantine_release_does_not_take_the_rest_of_the_sweep_with_it
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "undeliverable", duration_ms: 1, partitions_seen: 0,
      partitions_admitted: 0, partitions_denied: 0, jobs_admitted: 0,
      forward_failures: 0, pending_total: 0, inflight_total: 0, denied_reasons: {}
    )
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_tick_samples SET sampled_at = now() - interval '48 hours'"
    )

    boom = ->(**) { raise ActiveRecord::Deadlocked, "release lost the tie" }
    DispatchPolicy::Repository.stub(:release_aged_quarantines!, boom) do
      DispatchPolicy::TickLoop.sweep!
    end

    assert_equal 0, DispatchPolicy::TickSample.count,
                 "the retention sweep runs after the release; one rescue for the whole " \
                 "pass means a wedged release stops every other sweep forever"
  end
end
