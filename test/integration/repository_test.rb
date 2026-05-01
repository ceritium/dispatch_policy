# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

class RepositoryIntegrationTest < Minitest::Test
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
    ensure_schema!
    truncate_tables!
  end

  def ensure_schema!
    return if schema_present?

    drop_partial_schema!
    require_relative "../../db/migrate/20260501000001_create_dispatch_policy_tables"
    ActiveRecord::Migration.suppress_messages do
      CreateDispatchPolicyTables.new.change
    end
  end

  TABLES = %w[
    dispatch_policy_staged_jobs
    dispatch_policy_partitions
    dispatch_policy_inflight_jobs
    dispatch_policy_tick_samples
  ].freeze

  def schema_present?
    conn = ActiveRecord::Base.connection
    return false unless TABLES.all? { |t| conn.table_exists?(t) }

    # Detect schema drift (e.g. new column added in a migration update).
    cols = conn.columns("dispatch_policy_partitions").map(&:name)
    return false unless %w[total_admitted shard].all? { |c| cols.include?(c) }

    true
  end

  def drop_partial_schema!
    conn = ActiveRecord::Base.connection
    TABLES.each { |t| conn.execute("DROP TABLE IF EXISTS #{t} CASCADE") }
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE #{TABLES.join(", ")} RESTART IDENTITY"
    )
  end

  def test_stage_creates_staged_and_partition_rows
    DispatchPolicy::Repository.stage!(
      policy_name:   "p1",
      partition_key: "throttle=ep1",
      queue_name:    "default",
      job_class:     "MyJob",
      job_data:      { "job_id" => "j1", "job_class" => "MyJob", "arguments" => [] },
      context:       { "endpoint_id" => "ep1", "max" => 5 },
      scheduled_at:  nil,
      priority:      0
    )

    assert_equal 1, DispatchPolicy::StagedJob.count
    partition = DispatchPolicy::Partition.first
    assert_equal "p1",            partition.policy_name
    assert_equal "throttle=ep1",  partition.partition_key
    assert_equal 1,               partition.pending_count
    assert_equal({ "endpoint_id" => "ep1", "max" => 5 }, partition.context)
    refute_nil partition.context_updated_at
  end

  def test_context_is_refreshed_on_each_enqueue
    base = {
      policy_name:   "p1",
      partition_key: "throttle=ep1",
      queue_name:    "default",
      job_class:     "MyJob",
      job_data:      { "job_id" => "x", "job_class" => "MyJob", "arguments" => [] },
      scheduled_at:  nil,
      priority:      0
    }

    DispatchPolicy::Repository.stage!(**base.merge(context: { "max" => 5 }))
    DispatchPolicy::Repository.stage!(**base.merge(context: { "max" => 10 }))

    partition = DispatchPolicy::Partition.first
    assert_equal 2, partition.pending_count
    assert_equal({ "max" => 10 }, partition.context, "partition.context must reflect the most recent enqueue")
  end

  def test_claim_partitions_updates_last_checked_and_skips_locked
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k1", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )

    rows = DispatchPolicy::Repository.claim_partitions(policy_name: "p", limit: 5)
    assert_equal 1, rows.size
    assert_equal "k1", rows.first["partition_key"]

    persisted = DispatchPolicy::Partition.first
    refute_nil persisted.last_checked_at, "claim_partitions must bump last_checked_at"
  end

  def test_claim_staged_jobs_deletes_returning_and_updates_partition
    3.times do |i|
      DispatchPolicy::Repository.stage!(
        policy_name: "p", partition_key: "k", queue_name: nil,
        job_class: "J", job_data: { "job_id" => "j#{i}", "job_class" => "J", "arguments" => [] },
        context: { "max" => 3 }, priority: 0
      )
    end

    claimed = DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: "p", partition_key: "k", limit: 2,
      gate_state_patch: { "throttle" => { "tokens" => 1.0 } },
      retry_after: 5.0
    )

    assert_equal 2, claimed.size
    assert_equal 1, DispatchPolicy::StagedJob.count

    partition = DispatchPolicy::Partition.first
    assert_equal 1, partition.pending_count
    refute_nil partition.last_admit_at
    refute_nil partition.next_eligible_at
    assert_equal({ "throttle" => { "tokens" => 1.0 } }, partition.gate_state)
  end

  def test_claim_staged_jobs_with_zero_limit_still_records_evaluation
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )

    rows = DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: "p", partition_key: "k", limit: 0,
      gate_state_patch: { "throttle" => { "tokens" => 0.4 } },
      retry_after: 5.0
    )

    assert_empty rows
    partition = DispatchPolicy::Partition.first
    assert_equal 1, partition.pending_count, "no rows admitted, pending_count must be unchanged"
    refute_nil partition.next_eligible_at,
               "concurrency-full / throttle-denied evaluations must still set next_eligible_at"
    assert_in_delta 5.0, partition.next_eligible_at - Time.current, 1.0
    assert_equal({ "throttle" => { "tokens" => 0.4 } }, partition.gate_state)
  end

  def test_inflight_count_round_trip
    DispatchPolicy::Repository.insert_inflight!([
      { policy_name: "p", partition_key: "concurrency=acct:1", active_job_id: "abc" },
      { policy_name: "p", partition_key: "concurrency=acct:1", active_job_id: "def" }
    ])
    assert_equal 2, DispatchPolicy::Repository.count_inflight(policy_name: "p", partition_key: "concurrency=acct:1")

    DispatchPolicy::Repository.delete_inflight!(active_job_id: "abc")
    assert_equal 1, DispatchPolicy::Repository.count_inflight(policy_name: "p", partition_key: "concurrency=acct:1")
  end

  def test_total_admitted_increments_on_claim
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j2", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: "p", partition_key: "k", limit: 2,
      gate_state_patch: {}, retry_after: nil
    )
    assert_equal 2, DispatchPolicy::Partition.first.total_admitted
  end

  def test_record_tick_sample_and_summary
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "p", duration_ms: 30, partitions_seen: 5,
      partitions_admitted: 3, partitions_denied: 2,
      jobs_admitted: 12, forward_failures: 1,
      pending_total: 100, inflight_total: 4,
      denied_reasons: { "throttle_empty" => 2 }
    )
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "p", duration_ms: 40, partitions_seen: 6,
      partitions_admitted: 4, partitions_denied: 2,
      jobs_admitted: 8, forward_failures: 0,
      pending_total: 80, inflight_total: 4,
      denied_reasons: { "concurrency_full" => 1, "throttle_empty" => 1 }
    )

    summary = DispatchPolicy::Repository.tick_summary(policy_name: "p", since: Time.current - 60)
    assert_equal 20, summary[:jobs_admitted]
    assert_equal 11, summary[:partitions_seen]
    assert_equal 1,  summary[:forward_failures]
    assert_equal 2,  summary[:ticks]

    reasons = DispatchPolicy::Repository.denied_reasons_summary(policy_name: "p", since: Time.current - 60)
    assert_equal 3, reasons["throttle_empty"]
    assert_equal 1, reasons["concurrency_full"]
  end

  def test_tick_samples_buckets_returns_per_minute_aggregates
    base = { policy_name: "p", duration_ms: 0, partitions_seen: 0, partitions_admitted: 0,
             partitions_denied: 0, forward_failures: 0,
             pending_total: 0, inflight_total: 0, denied_reasons: {} }
    DispatchPolicy::Repository.record_tick_sample!(**base.merge(jobs_admitted: 5))
    DispatchPolicy::Repository.record_tick_sample!(**base.merge(jobs_admitted: 7))

    buckets = DispatchPolicy::Repository.tick_samples_buckets(
      policy_name: "p", since: Time.current - 60, bucket_seconds: 60
    )

    assert_equal 1, buckets.size, "two samples within the same minute → one bucket"
    assert_equal 12, buckets.first[:jobs_admitted]
    assert buckets.first[:bucket_at], "bucket_at must be present (date_bin result)"
  end

  def test_partition_round_trip_stats
    %w[k1 k2 k3 k4 k5].each do |k|
      DispatchPolicy::Repository.stage!(
        policy_name: "p", partition_key: k, queue_name: nil,
        job_class: "J", job_data: { "job_id" => k, "job_class" => "J", "arguments" => [] },
        context: {}, priority: 0
      )
    end

    # Spread last_checked_at: 60s, 30s, 10s, 5s, 1s ago.
    {"k1" => 60, "k2" => 30, "k3" => 10, "k4" => 5, "k5" => 1}.each do |key, ago|
      DispatchPolicy::Partition.where(partition_key: key).update_all(last_checked_at: ago.seconds.ago)
    end

    stats = DispatchPolicy::Repository.partition_round_trip_stats(policy_name: "p")
    assert_equal 5, stats[:active_partitions]
    assert_in_delta 60.0, stats[:oldest_age_seconds], 2.0

    # P95 must be at least as old as P50 (regression guard against earlier
    # SQL bug that inverted the percentile direction on timestamps).
    assert stats[:p95_age_seconds] >= stats[:p50_age_seconds],
           "P95 age (#{stats[:p95_age_seconds]}) must be >= P50 age (#{stats[:p50_age_seconds]})"

    assert stats[:oldest_age_seconds] >= stats[:p95_age_seconds],
           "oldest age (#{stats[:oldest_age_seconds]}) must be >= P95 (#{stats[:p95_age_seconds]})"
  end

  def test_sweep_old_tick_samples
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "p", duration_ms: 0, partitions_seen: 0, partitions_admitted: 0,
      partitions_denied: 0, jobs_admitted: 0, forward_failures: 0,
      pending_total: 0, inflight_total: 0, denied_reasons: {}
    )
    DispatchPolicy::TickSample.update_all(sampled_at: 2.days.ago)

    DispatchPolicy::Repository.sweep_old_tick_samples!(cutoff_seconds: 24 * 60 * 60)

    assert_equal 0, DispatchPolicy::TickSample.count
  end

  def test_cursor_pagination_walks_full_dataset_without_dups
    20.times do |i|
      DispatchPolicy::Repository.stage!(
        policy_name: "p", partition_key: "k#{i.to_s.rjust(2, '0')}", queue_name: nil,
        job_class: "J", job_data: { "job_id" => i.to_s, "job_class" => "J", "arguments" => [] },
        context: {}, priority: 0
      )
    end
    DispatchPolicy::Partition.find_each { |p| p.update!(pending_count: rand(0..5)) }

    seen = []
    cursor = nil
    page_size = 7
    100.times do
      scope = DispatchPolicy::Partition.all
      paginated = DispatchPolicy::CursorPagination.apply(scope, "pending", cursor)
      sort_def = DispatchPolicy::CursorPagination.sort_for("pending")
      rows = paginated.order(Arel.sql(sort_def[:sql_order])).limit(page_size + 1).to_a
      page = rows.first(page_size)
      seen.concat(page.map(&:id))

      break unless rows.size > page_size

      v, id  = DispatchPolicy::CursorPagination.extract(page.last, "pending")
      cursor = DispatchPolicy::CursorPagination.decode(
        DispatchPolicy::CursorPagination.encode(v, id)
      )
    end

    assert_equal 20, seen.size, "must walk every row"
    assert_equal seen.uniq, seen, "must not repeat rows across cursor pages"
  end

  def test_partitions_default_shard_when_not_specified
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    assert_equal "default", DispatchPolicy::Partition.first.shard
  end

  def test_claim_partitions_filters_by_shard
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k1", queue_name: nil, shard: "shard-a",
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k2", queue_name: nil, shard: "shard-b",
      job_class: "J", job_data: { "job_id" => "2", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )

    rows_a = DispatchPolicy::Repository.claim_partitions(policy_name: "p", shard: "shard-a", limit: 10)
    assert_equal ["k1"], rows_a.map { |r| r["partition_key"] }

    rows_b = DispatchPolicy::Repository.claim_partitions(policy_name: "p", shard: "shard-b", limit: 10)
    assert_equal ["k2"], rows_b.map { |r| r["partition_key"] }

    rows_all = DispatchPolicy::Repository.claim_partitions(policy_name: "p", limit: 10)
    assert_equal %w[k1 k2].sort, rows_all.map { |r| r["partition_key"] }.sort
  end

  def test_shard_is_pinned_on_first_write
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil, shard: "shard-a",
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil, shard: "shard-b",
      job_class: "J", job_data: { "job_id" => "2", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    assert_equal "shard-a", DispatchPolicy::Partition.first.shard,
                 "shard must be sticky to the first writer to avoid bouncing partitions across tick workers"
  end

  def test_concurrent_claim_partitions_uses_skip_locked
    # Stage one partition. Open a transaction that locks its row, then ensure
    # a second connection's claim_partitions returns nothing for it.
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )

    other = ActiveRecord::Base.connection_pool.checkout
    begin
      ActiveRecord::Base.connection.transaction(requires_new: true) do
        ActiveRecord::Base.connection.execute(
          "SELECT id FROM dispatch_policy_partitions WHERE policy_name = 'p' FOR UPDATE"
        )

        observed = nil
        Thread.new do
          ActiveRecord::Base.connection_pool.with_connection do
            observed = DispatchPolicy::Repository.claim_partitions(policy_name: "p", limit: 5)
          end
        end.join

        assert_equal 0, observed.size, "FOR UPDATE SKIP LOCKED must skip the row locked by another trx"
      end
    ensure
      ActiveRecord::Base.connection_pool.checkin(other) rescue nil
    end
  end
end
