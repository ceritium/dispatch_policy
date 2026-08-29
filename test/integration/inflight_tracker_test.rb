# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/inflight_job"

class InflightTrackerHeartbeatTest < DispatchPolicy::IntegrationTest
  def test_heartbeat_thread_refreshes_heartbeat_at_during_perform
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05

    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "p", partition_key: "k", active_job_id: "ajid-heartbeat"
    }])

    initial = DispatchPolicy::InflightJob
                .find_by(active_job_id: "ajid-heartbeat").heartbeat_at

    hb = DispatchPolicy::InflightTracker.start_heartbeat("ajid-heartbeat")
    sleep 0.3
    DispatchPolicy::InflightTracker.stop_heartbeat(hb)

    refreshed = DispatchPolicy::InflightJob
                  .find_by(active_job_id: "ajid-heartbeat").heartbeat_at

    assert refreshed > initial,
           "heartbeat_at should have advanced (#{initial.iso8601(3)} -> #{refreshed.iso8601(3)})"
  ensure
    DispatchPolicy.reset_config!
  end

  def test_heartbeat_stops_cleanly_after_perform
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05

    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "p", partition_key: "k", active_job_id: "ajid-stop"
    }])

    hb = DispatchPolicy::InflightTracker.start_heartbeat("ajid-stop")
    DispatchPolicy::InflightTracker.stop_heartbeat(hb)

    assert hb.stop_flag.true?, "stop_flag must be set after stop_heartbeat"
    refute hb.thread.alive?,   "heartbeat thread must terminate after stop"
  ensure
    DispatchPolicy.reset_config!
  end

  def test_heartbeat_disabled_when_interval_zero
    DispatchPolicy.config.inflight_heartbeat_interval = 0
    assert_nil DispatchPolicy::InflightTracker.start_heartbeat("ajid-zero")
  ensure
    DispatchPolicy.reset_config!
  end
  # The heartbeat thread runs outside the Rails executor, so the pool
  # treats its lease as permanent: `with_connection` marks it sticky and
  # then deliberately does NOT release it, on the assumption that whoever
  # established the lease will. Nothing did. One connection per running
  # tracked job is then pinned for the life of that job — and with the
  # Rails default sizing (pool and worker threads both from
  # RAILS_MAX_THREADS) that is the whole pool twice over, so the workers
  # start raising ConnectionTimeoutError and long jobs get swept as stale
  # while they are still running.
  def test_a_beat_returns_its_connection_to_the_pool
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "p", partition_key: "k", active_job_id: "ajid-pool"
    }])

    pool     = ActiveRecord::Base.connection_pool
    baseline = pool.stat[:busy]
    observed = nil

    thread = Thread.new do
      3.times { DispatchPolicy::InflightTracker.beat!("ajid-pool") }
      observed = ActiveRecord::Base.connection_pool.stat[:busy]
      sleep 0.4 # still alive, so a leaked connection would still be held
    end
    sleep 0.2
    during = pool.stat[:busy]
    thread.join

    assert_equal baseline, during,
                 "the beat's connection must be back in the pool while the thread lives on"
    assert_equal baseline, observed
  end
end
