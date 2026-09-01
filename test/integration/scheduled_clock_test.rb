# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/staged_job"

# A12/A11: scheduled work was compared on two clocks.
#
# Every datetime column in the gem is `timestamp WITHOUT time zone`.
# `scheduled_at` — and the `scheduled_eligible_at` horizon derived from it
# — is written by the application: ActiveJob's own timestamp, bound as a
# parameter and serialized by `quoted_date`. The comparisons that decide
# whether that work is due used `now()`, which is a timestamptz, so
# Postgres reinterpreted the stored value in the SESSION TimeZone.
#
# Rails sets that to UTC by default, which is why this hid: it only bites
# a host that sets `variables: { timezone: … }` in database.yml — a
# supported knob, commonly used to make raw psql output readable. Then
# every due-time comparison is off by the offset, in a direction that
# depends on its sign: a zone east of UTC runs `set(wait:)` jobs early, a
# zone west of it never runs them. Neither leaves a trace in any metric.
#
# The zones below are POSIX-style on purpose: `Etc/GMT-10` is UTC+10 and
# `Etc/GMT+10` is UTC-10 (the sign is inverted), and they have no DST, so
# the offset is the same whenever the suite runs.
class ScheduledClockTest < DispatchPolicy::IntegrationTest
  POLICY = "sched_clock"
  KEY    = "acct:1"
  EAST   = "Etc/GMT-10" # UTC+10 — makes now() look 10h LATER than it is
  WEST   = "Etc/GMT+10" # UTC-10 — makes now() look 10h EARLIER

  def teardown
    session_timezone("UTC")
    super
  end

  def session_timezone(tz)
    ActiveRecord::Base.connection.execute("SET TIME ZONE '#{tz}'")
  end

  def stage!(scheduled_at:)
    DispatchPolicy::Repository.stage!(
      policy_name: POLICY, partition_key: KEY, queue_name: nil,
      job_class: "X", job_data: { "job_id" => SecureRandom.uuid },
      context: {}, scheduled_at: scheduled_at
    )
  end

  def claim_staged(limit: 10)
    DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: POLICY, partition_key: KEY, limit: limit, retry_after: nil
    )
  end

  # The one that runs work early. A job due in an hour is not due.
  def test_a_future_job_is_not_claimed_under_an_eastward_session_timezone
    stage!(scheduled_at: Time.now.utc + 3600)
    session_timezone(EAST)

    assert_empty claim_staged,
                 "a job scheduled an hour out was admitted because the due-time " \
                 "comparison read the session TimeZone instead of the clock the " \
                 "timestamp was written on"
  end

  # The one that never runs work. A job due five minutes ago is due.
  def test_a_due_job_is_still_claimed_under_a_westward_session_timezone
    stage!(scheduled_at: Time.now.utc - 300)
    session_timezone(WEST)

    assert_equal 1, claim_staged.size,
                 "a job that is already due must be admitted whatever the session " \
                 "TimeZone is — read on the wrong clock it stays invisible until " \
                 "the offset elapses"
  end

  # Same rule one level up: the partition horizon `claim_partitions` reads
  # is an application-written timestamp too.
  def test_a_schedule_parked_partition_is_not_claimed_under_an_eastward_timezone
    stage!(scheduled_at: Time.now.utc + 3600)
    DispatchPolicy::Repository.defer_partition_to_next_scheduled!(
      policy_name: POLICY, partition_key: KEY
    )
    session_timezone(EAST)

    assert_empty DispatchPolicy::Repository.claim_partitions(policy_name: POLICY, limit: 10),
                 "the partition is parked behind work that is not due for an hour"
  end

  # The mirror case, and it has to start from a horizon that is genuinely
  # SET: a partition with `scheduled_eligible_at IS NULL` never evaluates
  # the comparison at all, so it would stay green against the bug.
  def test_a_due_partition_is_still_claimed_under_a_westward_timezone
    stage!(scheduled_at: Time.now.utc - 300)
    refute_nil DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
                                        .scheduled_eligible_at
    session_timezone(WEST)

    assert_equal 1,
                 DispatchPolicy::Repository.claim_partitions(policy_name: POLICY, limit: 10).size,
                 "work whose horizon has passed must stay claimable whatever the " \
                 "session TimeZone is — read on the wrong clock the partition sleeps " \
                 "through the offset"
  end

  # The scope the drain button counts what is left with has to agree with
  # the claim. Read on the session's clock it does not, and
  # `PartitionsController#drain` then flashes "N still pending — click
  # drain again" about rows nothing can move, on every click.
  def test_the_due_scope_agrees_with_the_claim_under_an_eastward_timezone
    stage!(scheduled_at: Time.now.utc + 3600)
    session_timezone(EAST)

    assert_equal 0, DispatchPolicy::StagedJob.for_partition(POLICY, KEY).due.count,
                 "the drain must not report as pending what the claim will not take"
    assert_empty claim_staged
  end

  def test_the_due_scope_still_counts_due_work_under_a_westward_timezone
    stage!(scheduled_at: Time.now.utc - 300)
    session_timezone(WEST)

    assert_equal 1, DispatchPolicy::StagedJob.for_partition(POLICY, KEY).due.count
    assert_equal 1, claim_staged.size
  end

  # `config.clock` is public API, and every other reader in the gem calls
  # `.to_f` on what it returns — so a lambda handing back an epoch Float
  # has always worked. Binding the clock into SQL must not narrow that as a
  # side effect: Postgres rejects "1788304522.524707" as a timestamp, and
  # the failure lands inside the admission path.
  def test_a_clock_that_returns_an_epoch_float_still_admits
    DispatchPolicy.config.clock = -> { Time.now.utc.to_f }
    stage!(scheduled_at: nil)

    assert_equal 1, DispatchPolicy::Repository.claim_partitions(policy_name: POLICY, limit: 10).size
    assert_equal 1, claim_staged.size
  end

  # `defer_partition_to_next_scheduled!` reads the same column from both
  # ends: MIN over rows still in the future, and a NOT EXISTS guard over
  # rows already due. On a skewed session both answers move together — the
  # future row reads as due, so the guard suppresses the park entirely and
  # the partition busy-loops every tick (M10, back again).
  #
  # It starts from a horizon of NULL on purpose. Staging a future job on a
  # fresh partition already writes the horizon in `upsert_partition!`, so a
  # test that only checks the value afterwards is asserting the UPSERT and
  # would pass with this whole statement deleted.
  def test_the_park_horizon_is_computed_on_the_writing_clock
    due_at = (Time.now.utc + 3600).change(usec: 0)
    stage!(scheduled_at: nil)     # due now -> horizon NULL
    stage!(scheduled_at: due_at)  # NULL is absorbing: still NULL
    assert_nil DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
                                        .scheduled_eligible_at
    assert_equal 1, claim_staged(limit: 1).size # the due one leaves; the future one stays

    session_timezone(EAST)
    DispatchPolicy::Repository.defer_partition_to_next_scheduled!(
      policy_name: POLICY, partition_key: KEY
    )

    horizon = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
                                       .scheduled_eligible_at
    refute_nil horizon, "the partition holds nothing but future work — it must be parked"
    assert_in_delta due_at.to_f, horizon.to_f, 1.0
  end
end
