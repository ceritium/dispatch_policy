# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"

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

  def schema_present?
    conn = ActiveRecord::Base.connection
    %w[dispatch_policy_staged_jobs dispatch_policy_partitions dispatch_policy_inflight_jobs].all? do |t|
      conn.table_exists?(t)
    end
  end

  def drop_partial_schema!
    conn = ActiveRecord::Base.connection
    %w[dispatch_policy_staged_jobs dispatch_policy_partitions dispatch_policy_inflight_jobs].each do |t|
      conn.execute("DROP TABLE IF EXISTS #{t} CASCADE")
    end
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(<<~SQL)
      TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, dispatch_policy_inflight_jobs RESTART IDENTITY
    SQL
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

  def test_unclaim_reinserts_rows
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    rows = DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: "p", partition_key: "k", limit: 1,
      gate_state_patch: {}, retry_after: nil
    )
    assert_equal 0, DispatchPolicy::StagedJob.count

    DispatchPolicy::Repository.unclaim!(rows)

    assert_equal 1, DispatchPolicy::StagedJob.count
    assert_equal 1, DispatchPolicy::Partition.first.pending_count
  end

  def test_unclaim_preserves_enqueued_at
    DispatchPolicy::Repository.stage!(
      policy_name: "p", partition_key: "k", queue_name: nil,
      job_class: "J", job_data: { "job_id" => "j", "job_class" => "J", "arguments" => [] },
      context: {}, priority: 0
    )
    original_enqueued_at = DispatchPolicy::StagedJob.first.enqueued_at

    rows = DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: "p", partition_key: "k", limit: 1,
      gate_state_patch: {}, retry_after: nil
    )
    sleep 0.05  # ensure now() would differ
    DispatchPolicy::Repository.unclaim!(rows)

    reinserted = DispatchPolicy::StagedJob.first
    assert_in_delta original_enqueued_at.to_f, reinserted.enqueued_at.to_f, 0.01,
                    "unclaim! must preserve original enqueued_at to keep FIFO ordering"
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
