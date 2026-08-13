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
end
