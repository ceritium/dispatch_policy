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
    # Polled rather than slept: the process-wide thread may be part-way
    # through a slice of a previously configured interval, so the first
    # beat under this test's cadence lands within a second, not instantly.
    deadline = Time.now + 5
    refreshed = nil
    loop do
      refreshed = DispatchPolicy::InflightJob
                    .find_by(active_job_id: "ajid-heartbeat").heartbeat_at
      break if refreshed > initial || Time.now > deadline

      sleep 0.05
    end
    DispatchPolicy::InflightTracker.stop_heartbeat(hb)

    assert refreshed > initial,
           "heartbeat_at should have advanced (#{initial.iso8601(3)} -> #{refreshed.iso8601(3)})"
  ensure
    DispatchPolicy.reset_config!
  end

  def test_stopping_the_last_job_retires_the_thread
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05

    token = DispatchPolicy::InflightTracker.start_heartbeat("ajid-stop")
    assert_equal "ajid-stop", token
    assert_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, token

    DispatchPolicy::InflightTracker.stop_heartbeat(token)
    refute_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, token,
                    "a stopped job must not keep getting beaten — its row is about to be deleted"

    deadline = Time.now + 5
    sleep 0.05 while heartbeat_threads.any? && Time.now < deadline
    assert_empty heartbeat_threads, "the thread must retire once nothing is running"
  ensure
    DispatchPolicy.reset_config!
  end

  # A12: ONE thread for the whole process, not one per running job.
  #
  # Per job, each thread checked out its own connection from a pool the
  # Rails default sizes to the worker's thread count — while every
  # performing job holds one for the length of its perform. A saturated
  # worker therefore had every beat queued behind `checkout_timeout`, and a
  # beat that never lands is a `heartbeat_at` that stops advancing, which
  # is what the stale sweeper reaps: it deletes the row of a job that is
  # still running and the concurrency gate re-admits against an occupied
  # slot.
  def test_every_running_job_shares_one_thread_and_one_statement
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    ids = %w[a b c]
    ids.each do |id|
      DispatchPolicy::Repository.insert_inflight!([{
        policy_name: "p", partition_key: "k", active_job_id: id
      }])
    end
    before = heartbeats_for(ids)

    beats = []
    sub = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      beats << payload[:sql] if payload[:name] == "heartbeat_inflight"
    end

    tokens = ids.map { |id| DispatchPolicy::InflightTracker.start_heartbeat(id) }
    assert_equal 1, heartbeat_threads.size,
                 "three running jobs must not mean three threads and three connections"

    deadline = Time.now + 5
    sleep 0.05 while beats.empty? && Time.now < deadline
    tokens.each { |t| DispatchPolicy::InflightTracker.stop_heartbeat(t) }

    assert_equal 1, beats.size, "all three rows in one statement, not one statement each"
    ids.each do |id|
      assert_operator heartbeats_for(ids)[id], :>, before[id], "#{id} was not beaten"
    end
  ensure
    ActiveSupport::Notifications.unsubscribe(sub) if sub
    DispatchPolicy.reset_config!
  end

  # A thread killed before `track`'s ensure never unregisters its job, and
  # the registry is process-wide: without a way to notice, the id would be
  # carried in every beat for the life of the worker. The beat's own
  # RETURNING answers which rows still exist.
  def test_a_job_whose_row_is_gone_stops_being_beaten
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "p", partition_key: "k", active_job_id: "ghost"
    }])
    DispatchPolicy::InflightTracker.start_heartbeat("ghost")
    DispatchPolicy::Repository.delete_inflight!(active_job_id: "ghost")

    deadline = Time.now + 5
    while DispatchPolicy::InflightTracker.heartbeat_ids.key?("ghost") && Time.now < deadline
      sleep 0.05
    end

    refute_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, "ghost"
  ensure
    DispatchPolicy::InflightTracker.stop_heartbeat("ghost")
    DispatchPolicy.reset_config!
  end

  def heartbeat_threads
    Thread.list.select { |t| t.alive? && t.name == DispatchPolicy::InflightTracker::HEARTBEAT_THREAD_NAME }
  end

  def heartbeats_for(ids)
    DispatchPolicy::InflightJob.where(active_job_id: ids)
                               .pluck(:active_job_id, :heartbeat_at).to_h
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
