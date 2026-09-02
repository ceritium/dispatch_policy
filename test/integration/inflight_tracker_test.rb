# frozen_string_literal: true

require_relative "../test_helper"
require "timeout"
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
    assert_equal "ajid-stop", token.active_job_id
    assert_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, token.active_job_id

    DispatchPolicy::InflightTracker.stop_heartbeat(token)
    refute_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, token.active_job_id,
                    "a stopped job must not keep getting beaten — its row is about to be deleted"

    quiesce_heartbeat_threads!
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
      next unless payload[:name] == "heartbeat_inflight"

      beats << payload[:binds].map { |b| b.respond_to?(:value) ? b.value : b }
    end

    tokens = ids.map { |id| DispatchPolicy::InflightTracker.start_heartbeat(id) }
    assert_equal 1, heartbeat_threads.size,
                 "three running jobs must not mean three threads and three connections"

    deadline = Time.now + 5
    sleep 0.05 while beats.empty? && Time.now < deadline
    tokens.each { |t| DispatchPolicy::InflightTracker.stop_heartbeat(t) }

    # The FIRST statement, not a count of them: counting races the loop —
    # a per-id implementation emits its second and third statements after
    # this thread has already been woken by the first, and the test then
    # passes against the bug. What the fix claims is that one statement
    # carries every running job, and the first one either does or does not.
    refute_empty beats, "the shared thread never beat"
    assert_equal ids.sort, beats.first.sort,
                 "all three rows in one statement, not one statement each"
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
    ghost = DispatchPolicy::InflightTracker.start_heartbeat("ghost")
    DispatchPolicy::Repository.delete_inflight!(active_job_id: "ghost")

    deadline = Time.now + 5
    while DispatchPolicy::InflightTracker.heartbeat_ids.key?("ghost") && Time.now < deadline
      sleep 0.05
    end

    refute_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, "ghost"
  ensure
    DispatchPolicy::InflightTracker.stop_heartbeat(ghost)
    DispatchPolicy.reset_config!
  end

  # A fork copies the module-level registry but not the thread. Beating the
  # parent's jobs from the child is worse than not beating them: it keeps
  # the inflight row of a job that is not running here fresh, so the stale
  # sweeper never reclaims it and the concurrency slot is lost for as long
  # as the child lives. Reproduced with a real fork (the child beat the
  # parent's job 2.6s after the parent stopped); pinned here on the pid
  # branch itself, which is the whole mechanism and does not need a forked
  # ActiveRecord connection to exercise.
  def test_a_process_that_inherits_the_registry_drops_it
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    parent = DispatchPolicy::InflightTracker.start_heartbeat("parents-job")
    assert_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, "parents-job"

    Process.stub(:pid, Process.pid + 1) do
      DispatchPolicy::InflightTracker.start_heartbeat("childs-job")
    end

    assert_equal %w[childs-job], DispatchPolicy::InflightTracker.heartbeat_ids.keys,
                 "a job running in the parent must not be heartbeated by the child"
  ensure
    # The pid switch orphans the thread the parent had started: it is still
    # alive, and the next test's "exactly one thread" assertion sees two.
    # Wait it out rather than leave the pollution for whoever runs next.
    stop_everything!
    quiesce_heartbeat_threads!
    DispatchPolicy.reset_config!
  end

  # `beat!` answers nil when it could not reach the database at all — the
  # ConnectionTimeoutError of a pool with no spare, most likely. Nil is "we
  # learned nothing", NOT "no rows survived": read as an empty survivor
  # list it unregisters every running job in the process on ONE transient
  # failure, permanently, and `inflight_stale_after` later has the sweeper
  # delete their rows while they run on — which is the over-admission this
  # whole thread exists to prevent.
  def test_a_beat_that_could_not_reach_the_database_unregisters_nobody
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    ids = %w[keep-1 keep-2]
    tokens = ids.map { |id| DispatchPolicy::InflightTracker.start_heartbeat(id) }

    attempts = Queue.new
    DispatchPolicy::InflightTracker.stub(:beat!, ->(_) { attempts << true; nil }) do
      2.times { attempts.pop }
    end

    assert_equal ids.sort, DispatchPolicy::InflightTracker.heartbeat_ids.keys.sort,
                 "a failed beat says nothing about which jobs are still running"
  ensure
    tokens&.each { |t| DispatchPolicy::InflightTracker.stop_heartbeat(t) }
    quiesce_heartbeat_threads!
    DispatchPolicy.reset_config!
  end

  # The other half of the same trade: one thread instead of one per job
  # means one uncaught error costs every running job in the process, not
  # one. The loop must survive a failing cycle rather than exit and wait
  # for some future job to reinstall it — a worker saturated with long jobs
  # never produces one.
  def test_the_loop_survives_a_failing_cycle
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "p", partition_key: "k", active_job_id: "survivor"
    }])
    before = heartbeats_for(["survivor"])["survivor"]
    survivor = DispatchPolicy::InflightTracker.start_heartbeat("survivor")

    boom = Queue.new
    DispatchPolicy::InflightTracker.stub(:beat!, ->(_) { boom << true; raise "cycle exploded" }) do
      boom.pop
    end

    deadline = Time.now + 8
    sleep 0.1 while heartbeats_for(["survivor"])["survivor"] <= before && Time.now < deadline

    assert heartbeat_threads.any?, "the thread must still be alive after a failing cycle"
    assert_operator heartbeats_for(["survivor"])["survivor"], :>, before,
                    "and must go back to beating once the failure clears"
  ensure
    DispatchPolicy::InflightTracker.stop_heartbeat(survivor)
    quiesce_heartbeat_threads!
    DispatchPolicy.reset_config!
  end

  # ActiveJob KEEPS the job_id across retries, so "the same id leaves and
  # comes back" is what `retry_on` does, not an exotic race. The beat's
  # pruning compares against a snapshot taken before the UPDATE: a retry
  # that registers in that window would be unregistered by the answer to a
  # question about the execution it replaced, and the live execution then
  # never beats again — five minutes later the sweeper deletes its row
  # while it runs.
  def test_a_retry_that_registers_during_a_beat_is_not_pruned
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    first = DispatchPolicy::InflightTracker.start_heartbeat("retried")

    # Widen the beat instead of reimplementing the pruning: the loop, the
    # snapshot and the `gone` computation are the production code, and only
    # the duration of the UPDATE is under the test's control. Asserting a
    # copy of the pruning line here would pass with the real one deleted.
    in_beat  = Queue.new
    proceed  = Queue.new
    retried  = nil
    registered = nil

    slow_beat = lambda do |_ids|
      in_beat << true
      proceed.pop
      [] # the row of the execution that FINISHED is gone
    end

    DispatchPolicy::InflightTracker.stub(:beat!, slow_beat) do
      await(in_beat)                    # cycle 1 is inside beat!
      DispatchPolicy::InflightTracker.stop_heartbeat(first)
      retried = DispatchPolicy::InflightTracker.start_heartbeat("retried")
      proceed << true                   # beat! answers []

      # Cycle 2 STARTING is the proof that cycle 1 finished, pruning
      # included. If cycle 1 pruned the retry the registry is empty, the
      # loop retires, and no cycle 2 ever comes — which is the bug, so
      # `await` fails the test rather than hanging.
      await(in_beat, "the loop retired: the retry was pruned and nothing beats it now")
      registered = DispatchPolicy::InflightTracker.heartbeat_ids.keys.dup
      proceed << true
    end

    assert_includes registered, "retried",
                    "the retry is a different execution and is still running"
  ensure
    DispatchPolicy::InflightTracker.stop_heartbeat(retried) if retried
    stop_everything!
    quiesce_heartbeat_threads!
    DispatchPolicy.reset_config!
  end

  # At-least-once delivery can put two deliveries of one job on the same
  # worker. With a thread per execution one could not stop the other's
  # heartbeat; collapsing them into one registry brought that back unless
  # the registry counts executions rather than ids.
  def test_stopping_one_execution_leaves_a_concurrent_one_registered
    DispatchPolicy.config.inflight_heartbeat_interval = 0.05
    a = DispatchPolicy::InflightTracker.start_heartbeat("twice")
    b = DispatchPolicy::InflightTracker.start_heartbeat("twice")

    DispatchPolicy::InflightTracker.stop_heartbeat(a)

    assert_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, "twice",
                    "the second delivery is still running and still needs beating"

    DispatchPolicy::InflightTracker.stop_heartbeat(b)
    refute_includes DispatchPolicy::InflightTracker.heartbeat_ids.keys, "twice"
  ensure
    stop_everything!
    quiesce_heartbeat_threads!
    DispatchPolicy.reset_config!
  end

  # Queue#pop(timeout:) is Ruby 3.2, and the gemspec floor is 3.1.
  def await(queue, message = "the heartbeat loop never got there")
    Timeout.timeout(10) { queue.pop }
  rescue Timeout::Error
    flunk message
  end

  def stop_everything!
    DispatchPolicy::InflightTracker.heartbeat_ids.dup.each do |id, seqs|
      seqs.each do |seq|
        DispatchPolicy::InflightTracker.stop_heartbeat(
          DispatchPolicy::InflightTracker::Registration.new(id, seq)
        )
      end
    end
  end

  def quiesce_heartbeat_threads!
    deadline = Time.now + 5
    sleep 0.05 while heartbeat_threads.any? && Time.now < deadline
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
