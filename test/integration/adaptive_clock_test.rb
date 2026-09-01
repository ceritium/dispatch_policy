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
      "VALUES ('adaptive_clock', 'k', $1, now() - interval '5 seconds', now())",
      "seed_inflight", [active_job_id]
    )
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
      DispatchPolicy::InflightTracker.track(job) { sleep 0.5 }
    end

    assert_operator observed.first[:queue_lag_ms], :<, 5_400,
                    "perform duration must not leak into the queue-wait signal"
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
