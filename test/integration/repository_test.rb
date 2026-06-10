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
    dispatch_policy_adaptive_concurrency_stats
    dispatch_policy_policy_settings
  ].freeze

  def schema_present?
    conn = ActiveRecord::Base.connection
    return false unless TABLES.all? { |t| conn.table_exists?(t) }

    # Detect schema drift (e.g. new column added in a migration update).
    cols = conn.columns("dispatch_policy_partitions").map(&:name)
    return false unless %w[total_admitted shard decayed_admits decayed_admits_at].all? { |c| cols.include?(c) }

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

  # M6: pausing a policy must hold partitions created AFTER the pause too,
  # not only the ones that existed when the operator clicked. claim_partitions
  # reads the policy_settings flag, so a brand-new active partition with
  # pending work is still skipped while the policy is paused.
  def test_claim_partitions_respects_policy_pause_for_new_partitions
    DispatchPolicy::Repository.set_policy_paused!(policy_name: "p", paused: true)

    # This partition is created (active) only after the pause.
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "fresh", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )

    assert_empty DispatchPolicy::Repository.claim_partitions(policy_name: "p", limit: 5),
                 "a paused policy must not admit a partition created after the pause"

    DispatchPolicy::Repository.set_policy_paused!(policy_name: "p", paused: false)
    assert_equal 1, DispatchPolicy::Repository.claim_partitions(policy_name: "p", limit: 5).size,
                 "resuming the policy makes its partitions claimable again"
  end

  # L1: stage_many! must chunk so a batch larger than PG's bind-param limit
  # (65_535 / 8 params per row ≈ 8_191 rows) doesn't fail the whole INSERT.
  # 2_500 rows crosses two STAGE_MANY_BATCH (1_000) boundaries cheaply.
  def test_stage_many_chunks_large_batches
    rows = Array.new(2_500) do |i|
      {
        policy_name:   "bulk",
        partition_key: "k",
        queue_name:    nil,
        job_class:     "J",
        job_data:      { "job_id" => "j#{i}", "job_class" => "J", "arguments" => [] },
        context:       {},
        priority:      0
      }
    end

    inserted = DispatchPolicy::Repository.stage_many!(rows)

    assert_equal 2_500, inserted
    assert_equal 2_500, DispatchPolicy::StagedJob.where(policy_name: "bulk").count
    assert_equal 2_500, DispatchPolicy::Partition.find_by(policy_name: "bulk", partition_key: "k").pending_count
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

  def test_claim_staged_jobs_rejects_non_positive_limit
    assert_raises(ArgumentError) do
      DispatchPolicy::Repository.claim_staged_jobs!(
        policy_name: "p", partition_key: "k", limit: 0,
        gate_state_patch: {}, retry_after: nil
      )
    end
  end

  def test_bulk_record_partition_denies_updates_gate_state_and_backoff
    # Pre-create two partitions with an existing gate_state key that must
    # survive the bulk UPDATE — proving `||` jsonb merge (not replacement).
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "a", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "1", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "b", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "2", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_partitions SET gate_state = '{\"foo\":\"keep-me\"}'::jsonb"
    )

    DispatchPolicy::Repository.bulk_record_partition_denies!([
      { policy_name: "p", partition_key: "a",
        gate_state_patch: { "throttle" => { "tokens" => 0.1 } }, retry_after: 5.0 },
      { policy_name: "p", partition_key: "b",
        gate_state_patch: { "concurrency" => { "full" => true } }, retry_after: nil }
    ])

    partitions = DispatchPolicy::Partition.order(:partition_key).to_a
    a, b = partitions

    # Per-partition gate_state was merged with its patch via jsonb ||,
    # not replaced; pending_count stayed put (deny doesn't decrement);
    # next_eligible_at was set when retry_after was given, NULL otherwise.
    assert_equal "keep-me", a.gate_state["foo"], "|| merge must keep pre-existing keys"
    assert_equal({ "tokens" => 0.1 }, a.gate_state["throttle"])
    assert_in_delta 5.0, a.next_eligible_at - Time.current, 1.0

    assert_equal "keep-me", b.gate_state["foo"]
    assert_equal({ "full" => true }, b.gate_state["concurrency"])
    assert_nil b.next_eligible_at, "retry_after: nil must clear next_eligible_at"

    assert_equal 1, a.pending_count
    assert_equal 1, b.pending_count
  end

  def test_bulk_record_partition_denies_no_op_on_empty
    DispatchPolicy::Repository.bulk_record_partition_denies!([])
    # Just doesn't raise.
    assert true
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

  def test_tick_summaries_by_policy_matches_per_policy_summary
    since = Time.current - 60
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "a", duration_ms: 30, partitions_seen: 5, partitions_admitted: 3,
      partitions_denied: 2, jobs_admitted: 12, forward_failures: 1,
      pending_total: 0, inflight_total: 0, denied_reasons: {}
    )
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "a", duration_ms: 50, partitions_seen: 1, partitions_admitted: 1,
      partitions_denied: 0, jobs_admitted: 8, forward_failures: 0,
      pending_total: 0, inflight_total: 0, denied_reasons: {}
    )
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "b", duration_ms: 10, partitions_seen: 1, partitions_admitted: 1,
      partitions_denied: 0, jobs_admitted: 3, forward_failures: 2,
      pending_total: 0, inflight_total: 0, denied_reasons: {}
    )

    grouped = DispatchPolicy::Repository.tick_summaries_by_policy(since: since)

    %w[a b].each do |name|
      single = DispatchPolicy::Repository.tick_summary(policy_name: name, since: since)
      assert_equal single[:jobs_admitted],    grouped[name][:jobs_admitted]
      assert_equal single[:forward_failures], grouped[name][:forward_failures]
      assert_equal single[:ticks],            grouped[name][:ticks]
      assert_equal single[:avg_duration_ms],  grouped[name][:avg_duration_ms]
    end
    assert_equal 20, grouped["a"][:jobs_admitted]
    assert_equal 3,  grouped["b"][:jobs_admitted]
  end

  def test_top_denied_reason_by_policy_picks_the_max_per_policy
    since = Time.current - 60
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "a", duration_ms: 0, partitions_seen: 0, partitions_admitted: 0,
      partitions_denied: 0, jobs_admitted: 0, forward_failures: 0,
      pending_total: 0, inflight_total: 0,
      denied_reasons: { "throttle_empty" => 5, "concurrency_full" => 2 }
    )
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "a", duration_ms: 0, partitions_seen: 0, partitions_admitted: 0,
      partitions_denied: 0, jobs_admitted: 0, forward_failures: 0,
      pending_total: 0, inflight_total: 0,
      denied_reasons: { "concurrency_full" => 9 }
    )
    DispatchPolicy::Repository.record_tick_sample!(
      policy_name: "b", duration_ms: 0, partitions_seen: 0, partitions_admitted: 0,
      partitions_denied: 0, jobs_admitted: 0, forward_failures: 0,
      pending_total: 0, inflight_total: 0,
      denied_reasons: { "tick_cap_exhausted" => 1 }
    )

    top = DispatchPolicy::Repository.top_denied_reason_by_policy(since: since)
    # a: concurrency_full 11 > throttle_empty 5
    assert_equal ["concurrency_full", 11], top["a"]
    assert_equal ["tick_cap_exhausted", 1], top["b"]
  end

  def test_partition_counts_by_policy_matches_per_policy_aggregates
    %w[a a b].each_with_index do |name, i|
      DispatchPolicy::Repository.stage!(
        policy_name: name, partition_key: "k#{i}", queue_name: nil,
        job_class: "J", job_data: { "job_id" => "#{name}#{i}", "job_class" => "J", "arguments" => [] },
        context: {}, priority: 0
      )
    end
    # Pause one of a's partitions.
    DispatchPolicy::Partition.where(policy_name: "a", partition_key: "k0").update_all(status: "paused")

    grouped = DispatchPolicy::Repository.partition_counts_by_policy

    %w[a b].each do |name|
      scope = DispatchPolicy::Partition.for_policy(name)
      assert_equal scope.sum(:pending_count), grouped[name][:pending], "#{name} pending"
      assert_equal scope.count,               grouped[name][:partitions], "#{name} partitions"
      assert_equal scope.paused.count,        grouped[name][:paused], "#{name} paused"
    end
    assert_equal 2, grouped["a"][:partitions]
    assert_equal 1, grouped["a"][:paused]
    assert_equal 1, grouped["b"][:partitions]
    assert_equal 0, grouped["b"][:paused]
  end

  def test_partition_round_trip_stats_by_policy_matches_per_policy
    %w[a b].each do |name|
      DispatchPolicy::Repository.stage!(
        policy_name: name, partition_key: "k", queue_name: nil,
        job_class: "J", job_data: { "job_id" => "#{name}1", "job_class" => "J", "arguments" => [] },
        context: {}, priority: 0
      )
    end
    DispatchPolicy::Partition.where(policy_name: "a").update_all(last_checked_at: 60.seconds.ago)
    DispatchPolicy::Partition.where(policy_name: "b").update_all(
      last_checked_at: 10.seconds.ago, next_eligible_at: 30.seconds.from_now
    )

    grouped = DispatchPolicy::Repository.partition_round_trip_stats_by_policy

    %w[a b].each do |name|
      single = DispatchPolicy::Repository.partition_round_trip_stats(policy_name: name)
      assert_equal single[:in_backoff], grouped[name][:in_backoff]
      assert_in_delta single[:oldest_age_seconds], grouped[name][:oldest_age_seconds], 1.0
      assert_in_delta single[:p95_age_seconds],    grouped[name][:p95_age_seconds], 1.0
    end
    assert_equal 0, grouped["a"][:in_backoff]
    assert_equal 1, grouped["b"][:in_backoff], "b's partition is in backoff (next_eligible_at in future)"
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

  # Regression test: a partition that has sat idle for many half-lives
  # produces a Δt/τ ratio large enough to underflow `exp()` on double
  # precision (Postgres throws `value out of range: underflow` around
  # `exp(-746)`). Before the clamp, this broke every admission UPDATE
  # on the partition forever — Tick rolled the TX back, the staged
  # rows returned, and the partition never drained.
  def test_record_partition_admit_survives_an_ancient_decayed_admits_at
    DispatchPolicy::Repository.stage!(
      policy_name:   "p_decay",
      partition_key: "k",
      queue_name:    nil,
      job_class:     "MyJob",
      job_data:      { "job_id" => "j1", "job_class" => "MyJob", "arguments" => [] },
      context:       {},
      scheduled_at:  nil,
      priority:      0
    )

    # Push decayed_admits_at far enough into the past that
    # exp(-Δt/τ) would underflow without the clamp. With half_life = 60s,
    # τ ≈ 86.56; 30 days of idleness yields Δt/τ ≈ 30_000, well past the
    # ~746 PG underflow threshold.
    DispatchPolicy::Partition.where(policy_name: "p_decay").update_all(
      decayed_admits:    1.0,
      decayed_admits_at: 30.days.ago
    )

    DispatchPolicy::Repository.record_partition_admit!(
      policy_name:       "p_decay",
      partition_key:     "k",
      admitted:          1,
      gate_state_patch:  {},
      retry_after:       nil,
      half_life_seconds: 60
    )

    partition = DispatchPolicy::Partition.find_by(policy_name: "p_decay")
    refute_nil partition, "partition row must still exist after the admit"
    # The decay factor collapses to ~0, so decayed_admits ≈ admitted (1).
    assert_in_delta 1.0, partition.decayed_admits, 1e-9,
                    "very-stale partition admit must reset decayed_admits to ~admitted, not raise"
  end
end
