# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/adaptive_concurrency_stats"

# A10: the adaptive gate's feedback signal was subtracted across two
# clocks.
#
# `admitted_at` is written by Postgres `now()` on the tick process's
# connection; the queue lag used to be `Time.current - admitted_at`,
# measured on the worker's. The gate is an AIMD controller whose entire
# input is that number: a worker whose clock runs ahead of the database by
# more than `target_lag_ms` reads EVERY job as late, shrinks `current_max`
# on every observation and never grows it back — the cap collapses to
# `min` and stays there, with nothing anywhere reporting a clock problem.
# The same offset also comes from the two ends disagreeing about the
# session TimeZone, since these are `timestamp WITHOUT time zone` columns.
class AdaptiveClockTest < DispatchPolicy::IntegrationTest
  class AdaptiveJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("adaptive_clock") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :adaptive_concurrency, initial_max: 4, target_lag_ms: 1_000
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(AdaptiveJob::POLICY)
    DispatchPolicy.config.inflight_heartbeat_interval = 0
  end

  # Admitted five seconds ago by the database's clock. The worker then
  # believes it is an hour later.
  def admit!(active_job_id)
    ActiveRecord::Base.connection.exec_query(
      "INSERT INTO dispatch_policy_inflight_jobs " \
      "(policy_name, partition_key, active_job_id, admitted_at, heartbeat_at) " \
      "VALUES ('adaptive_clock', 'k', $1, #{DispatchPolicy::Repository::UTC_NOW} - " \
      "interval '5 seconds', #{DispatchPolicy::Repository::UTC_NOW})",
      "seed_inflight", [active_job_id]
    )
  end

  # The write and the read are one subtraction, so a skewed session must not
  # move either end. It moved one: `admitted_at` went to UTC while
  # `lookup_admission` still cast `clock_timestamp()` in the session's zone,
  # and the lag came back 36,000,000ms against a true 1ms. The fixture above
  # is what hid it — it seeded with a bare `now()`, i.e. the store shape
  # production had stopped writing.
  { "east" => "Etc/GMT-10", "west" => "Etc/GMT+10", "half_hour" => "Asia/Kathmandu" }
    .each do |direction, zone|
    define_method("test_the_queue_lag_ignores_a_skewed_session_#{direction}") do
      DispatchPolicy::Repository.connection.execute("SET TIME ZONE '#{zone}'")
      job = AdaptiveJob.new
      admit!(job.job_id)

      observed = []
      gate = AdaptiveJob::POLICY.gates.find { |g| g.name == :adaptive_concurrency }
      gate.stub(:record_observation, ->(**kwargs) { observed << kwargs }) do
        DispatchPolicy::InflightTracker.track(job) { nil }
      end

      assert_in_delta 5_000, observed.first[:queue_lag_ms], 2_000,
                      "#{direction}: the job waited five seconds; the session's TimeZone " \
                      "must not reach the AIMD controller's only input"
    ensure
      DispatchPolicy::Repository.connection.execute("SET TIME ZONE 'UTC'")
    end
  end

  def test_the_queue_lag_comes_from_the_database_not_the_workers_clock
    job = AdaptiveJob.new
    admit!(job.job_id)

    observed = []
    gate = AdaptiveJob::POLICY.gates.find { |g| g.name == :adaptive_concurrency }
    capture = ->(**kwargs) { observed << kwargs }

    travel_to(Time.now.utc + 3600) do
      gate.stub(:record_observation, capture) do
        DispatchPolicy::InflightTracker.track(job) { nil }
      end
    end

    assert_equal 1, observed.size
    assert_in_delta 5_000, observed.first[:queue_lag_ms], 2_000,
                    "the job waited five seconds; an hour of clock skew between the " \
                    "worker and the database must not reach the AIMD controller"
  end

  # The lag must be the QUEUE wait, not the perform duration — it is read
  # before the block runs, and that has to stay true.
  def test_the_lag_excludes_the_time_the_job_spends_performing
    job = AdaptiveJob.new
    admit!(job.job_id)

    observed = []
    gate = AdaptiveJob::POLICY.gates.find { |g| g.name == :adaptive_concurrency }

    gate.stub(:record_observation, ->(**kwargs) { observed << kwargs }) do
      DispatchPolicy::InflightTracker.track(job) { sleep 1.5 }
    end

    # A RANGE, not an upper bound. The queue wait is unknown-safe: a failed
    # lookup is recorded as a lag of 0, and 0 satisfies any `<` assertion —
    # so `assert_operator :<` passes both when the fix works and when the
    # measurement silently stops happening. Admitted 5s ago means ~5000ms;
    # a lag read AFTER the block would be ~6500.
    assert_in_delta 5_000, observed.first[:queue_lag_ms], 1_200,
                    "perform duration must not leak into the queue-wait signal"
  end

  # `clock_timestamp()`, not `now()`: `now()` is the TRANSACTION timestamp
  # and stops advancing inside an open transaction, so a host that wraps
  # the perform in one — Rails transactional tests, among others — would
  # report the queue wait as "time since that transaction opened" and the
  # AIMD controller would never see a job as late.
  def test_the_lag_is_wall_clock_not_the_enclosing_transactions_timestamp
    job = AdaptiveJob.new
    admit!(job.job_id) # 5s ago

    observed = []
    gate = AdaptiveJob::POLICY.gates.find { |g| g.name == :adaptive_concurrency }

    DispatchPolicy::Repository.connection.transaction do
      # Rails opens transactions lazily, so BEGIN is not issued until a
      # statement needs it. Without this the BEGIN lands AFTER the sleep,
      # now() and clock_timestamp() agree, and the test passes on both.
      DispatchPolicy::Repository.connection.select_value("SELECT 1")
      sleep 1.5 # inside the transaction now() no longer moves; the wall clock does
      gate.stub(:record_observation, ->(**kwargs) { observed << kwargs }) do
        DispatchPolicy::InflightTracker.track(job) { nil }
      end
    end

    assert_in_delta 6_500, observed.first[:queue_lag_ms], 900,
                    "on now() this reads 5000 — the transaction's own timestamp minus " \
                    "admitted_at — however long the job actually waited"
  end

  # No row (swept, or a policy the Tick never pre-inserted for) means the
  # wait is unknown, not zero-length work: the observation is still
  # recorded so sample_count advances and the cap can grow.
  def test_a_missing_admission_row_records_a_zero_lag_rather_than_nothing
    job = AdaptiveJob.new

    observed = []
    gate = AdaptiveJob::POLICY.gates.find { |g| g.name == :adaptive_concurrency }
    gate.stub(:record_observation, ->(**kwargs) { observed << kwargs }) do
      DispatchPolicy::InflightTracker.stub(:lookup_admission, [nil, nil]) do
        DispatchPolicy::InflightTracker.track(job) { nil }
      end
    end

    assert_equal 1, observed.size
    assert_equal 0, observed.first[:queue_lag_ms]
  end
end
