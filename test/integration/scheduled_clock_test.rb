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

  # Seeded with the same expression the gem writes with, so the only
  # thing a skewed session can change is whether the READ is right.
  UTC = DispatchPolicy::Repository::UTC_NOW

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

  # The mirror image of A11, and the one CLAUDE.md got wrong: `sampled_at`
  # was written by Postgres `now()` while every reader of it — the
  # dashboard's 1m/5m/15m windows, the sparkline, the denial breakdown and
  # the retention sweep — bounds on a Ruby `Time`. On a session west of
  # UTC the sample lands ten hours in the past and the dashboard shows an
  # idle tick loop; east of UTC it never ages out.
  def test_a_tick_sample_written_on_a_skewed_session_is_still_in_the_window
    session_timezone(WEST)
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: POLICY, duration_ms: 5, partitions_seen: 1, partitions_admitted: 1,
      partitions_denied: 0, jobs_admitted: 3, forward_failures: 0,
      pending_total: 0, inflight_total: 0, denied_reasons: {}
    )

    summary = DispatchPolicy::Repository.tick_summary(policy_name: POLICY, since: Time.now.utc - 60)
    assert_equal 1, summary[:ticks],
                 "the tick that just ran must appear in the last minute, whatever the " \
                 "session TimeZone is"
    assert_equal 3, summary[:jobs_admitted]
  end

  # The in-tick fairness reorder decays by the time since
  # `decayed_admits_at`, a column the DATABASE writes. Computing that
  # elapsed time from `Time.current` puts the two ends of one subtraction
  # on two MACHINES' clocks — the worker's and the database's — and the
  # order it produces is only as good as their agreement.
  #
  # `travel_to` is the discriminator, not a skewed session TimeZone. Since
  # every column is stored in UTC (see `Repository::UTC_NOW`), a skewed
  # session no longer makes Ruby and Postgres disagree about a stored
  # value, so that version of this test stopped discriminating the moment
  # A13 was fixed — it SURVIVED its own mutation.
  #
  # BEHIND, specifically, and the direction is the whole test. A worker
  # ahead of the database is harmless here: it adds the same constant to
  # every elapsed time, `exp(-(e+d)/tau)` factors into
  # `exp(-e/tau) * exp(-d/tau)`, and multiplying every sort key by one
  # positive constant cannot reorder them. A worker BEHIND drives elapsed
  # negative, where `[…, 0.0].max` clamps it — and a clamp is not uniform:
  # every partition collapses to its RAW admit count, so the one that just
  # bursted sorts last instead of first. Getting this backwards is how the
  # first version of this test passed against its own mutation.
  def test_the_fairness_order_survives_a_worker_clock_that_has_drifted
    seed_decayed!("busy",  admits: 10.0, ago: 600) # bursted, then idle 10 min
    seed_decayed!("quiet", admits: 1.0,  ago: 0)   # just admitted a little

    # A gate-less policy is enough: the reorder is the subject, and it runs
    # whether or not any gate does.
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build(POLICY) do
        context ->(_args) { {} }
        partition_by ->(_c) { "k" }
      end
    )

    claimed = DispatchPolicy::Repository.claim_partitions(policy_name: POLICY, limit: 10)

    # This process's clock is ten hours BEHIND the database's.
    travel_to(Time.now.utc - (10 * 3600)) do
      DispatchPolicy::Tick.new(POLICY).send(:sort_partitions_for_fairness!, claimed)
    end

    assert_equal %w[busy quiet], claimed.map { |p| p["partition_key"] },
                 "after 10 idle minutes `busy` has decayed below `quiet` and must go first; " \
                 "computed on this process's clock the elapsed time goes negative, clamps " \
                 "to zero, and every partition sorts on its raw admit count instead"
  end

  def seed_decayed!(key, admits:, ago:)
    DispatchPolicy::Repository.stage!(
      policy_name: POLICY, partition_key: key, queue_name: nil,
      job_class: "X", job_data: {}, context: {}
    )
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET decayed_admits = $3, " \
      "decayed_admits_at = #{UTC} - ($4 || ' seconds')::interval " \
      "WHERE policy_name = $1 AND partition_key = $2",
      "seed_decay", [POLICY, key, admits, ago.to_i]
    )
  end

  # The four facts the partition page renders come from columns the
  # DATABASE writes, so the database computes them. Recomputing in Ruby
  # subtracts the worker's clock from the database's, and the page then
  # disagrees with the admission it is describing.
  #
  # The discriminator is host clock drift, not a skewed session: every
  # column is stored in UTC now (`Repository::UTC_NOW`), so a skewed
  # session no longer makes Ruby and Postgres disagree about a stored
  # value — the session-TimeZone version of these tests SURVIVED its own
  # mutations once A13 was fixed. Both directions, because they fail
  # differently: ahead, the age inflates and a live backoff still reads
  # live; behind, the age goes negative and a live backoff reads as none.
  { "ahead" => 10 * 3600, "behind" => -10 * 3600 }.each do |direction, drift|
    define_method("test_the_partition_page_facts_ignore_a_worker_clock_#{direction}") do
      stage!(scheduled_at: nil)
      DispatchPolicy::Repository.connection.exec_query(
        "UPDATE dispatch_policy_partitions SET decayed_admits = 10.0, " \
        "decayed_admits_at = #{UTC} - interval '600 seconds', " \
        "last_checked_at   = #{UTC} - interval '30 seconds', " \
        "next_eligible_at  = #{UTC} + interval '300 seconds' " \
        "WHERE policy_name = $1 AND partition_key = $2",
        "seed_facts", [POLICY, KEY]
      )

      facts = travel_to(Time.now.utc + drift) do
        DispatchPolicy::Repository.partition_clock_facts(policy_name: POLICY, partition_key: KEY)
      end

      assert facts[:in_backoff],
             "#{direction}: the tick will not claim this partition for another five minutes"
      assert_in_delta 30, facts[:age_seconds], 5,
                      "#{direction}: computed on this process's clock the age is off by the drift"
      assert_in_delta 600, facts[:decay_elapsed_seconds], 5,
                      "#{direction}: the page's EWMA is the Tick's own sort key"
    end
  end

  # `in_backoff` needs a backoff that has EXPIRED, and the drift has to
  # point the other way than it does for a live one. The comparison is
  # `next_eligible_at > clock`: a LIVE backoff is exposed by a clock that
  # runs AHEAD (it jumps past the deadline and the backoff vanishes), an
  # EXPIRED one by a clock that runs BEHIND (it falls back before the
  # deadline and a dead backoff comes back to life). This case drifted
  # forward at first, where the buggy and correct answers agree, and passed
  # against its own mutation.
  def test_an_expired_backoff_does_not_read_as_active_under_clock_drift
    stage!(scheduled_at: nil)
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET next_eligible_at = #{UTC} - interval '300 seconds' " \
      "WHERE policy_name = $1 AND partition_key = $2",
      "seed_expired", [POLICY, KEY]
    )

    facts = travel_to(Time.now.utc - (10 * 3600)) do
      DispatchPolicy::Repository.partition_clock_facts(policy_name: POLICY, partition_key: KEY)
    end

    refute facts[:in_backoff],
           "the backoff expired five minutes ago; computed on a clock ten hours behind the " \
           "database's it reads as active again and the tick is told to wait"
  end

  # `defer_partition_to_next_scheduled!` reads the same column from both  # `defer_partition_to_next_scheduled!` reads the same column from both
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
