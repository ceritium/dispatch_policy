# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/inflight_job"

# sweep_stale_inflight! must not reap rows for jobs that were admitted
# (pre-inserted by the Tick) but are still waiting in the adapter's queue.
# Their heartbeat_at never advances past admitted_at because the heartbeat
# thread only starts once a worker begins performing. Reaping them at the
# short cutoff would make the concurrency gate under-count and over-admit.
class InflightSweepTest < Minitest::Test
  def self.connect!
    return @connected if defined?(@connected) && @connected

    ActiveRecord::Base.establish_connection(
      adapter:  "postgresql",
      encoding: "unicode",
      host:     ENV.fetch("DB_HOST", "localhost"),
      username: ENV.fetch("DB_USER", ENV["USER"]),
      password: ENV.fetch("DB_PASS", ""),
      database: ENV.fetch("DB_NAME", "dispatch_policy_test")
    )
    ActiveRecord::Base.connection.execute("SELECT 1")
    @connected = true
  rescue StandardError => e
    warn "[skip] Postgres not reachable: #{e.message}"
    @connected = false
  end

  def setup
    super
    skip "no Postgres available" unless self.class.connect!
    ActiveRecord::Base.connection.execute("DELETE FROM dispatch_policy_inflight_jobs")
  end

  # admitted_at / heartbeat_at expressed as "seconds ago".
  def insert_row!(active_job_id, admitted_ago:, heartbeat_ago:)
    ActiveRecord::Base.connection.exec_query(
      <<~SQL.squish,
        INSERT INTO dispatch_policy_inflight_jobs
          (policy_name, partition_key, active_job_id, admitted_at, heartbeat_at)
        VALUES ('p', 'k', $1,
                now() - ($2 || ' seconds')::interval,
                now() - ($3 || ' seconds')::interval)
      SQL
      "insert_test_inflight",
      [active_job_id, admitted_ago.to_i, heartbeat_ago.to_i]
    )
  end

  def ids
    DispatchPolicy::InflightJob.order(:active_job_id).pluck(:active_job_id)
  end

  def test_two_tier_sweep_keeps_queued_jobs_but_reaps_dead_and_ancient
    # Started then died: heartbeat advanced past admission, then went silent
    # 10 min ago. cutoff_seconds=5min → reap.
    insert_row!("started_dead", admitted_ago: 1200, heartbeat_ago: 600)

    # Queued, not started: heartbeat_at == admitted_at, 10 min old. Past the
    # short 5-min cutoff but well within the 1h queued cutoff → KEEP.
    insert_row!("queued_recent", admitted_ago: 600, heartbeat_ago: 600)

    # Queued but ancient: heartbeat_at == admitted_at, 2h old. Past the 1h
    # queued cutoff → reap (admission presumed lost).
    insert_row!("queued_ancient", admitted_ago: 7200, heartbeat_ago: 7200)

    # Healthy running job: heartbeated 10s ago, past admission → KEEP.
    insert_row!("running_ok", admitted_ago: 300, heartbeat_ago: 10)

    DispatchPolicy::Repository.sweep_stale_inflight!(
      cutoff_seconds:        5 * 60,
      queued_cutoff_seconds: 60 * 60
    )

    assert_equal %w[queued_recent running_ok], ids,
                 "must reap started-then-dead and ancient-queued, keep queued-recent and running"
  end

  def test_queued_cutoff_defaults_to_short_cutoff_when_omitted
    # Back-compat: with no queued cutoff, a queued row past the short cutoff
    # is reaped (old single-tier behaviour).
    insert_row!("queued_recent", admitted_ago: 600, heartbeat_ago: 600)

    DispatchPolicy::Repository.sweep_stale_inflight!(cutoff_seconds: 5 * 60)

    assert_empty ids, "without a queued cutoff, the short cutoff applies to never-started rows too"
  end
end
