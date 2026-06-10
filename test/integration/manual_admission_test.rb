# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# Verifies the UI force-admit path (ManualAdmission.force!, behind the
# "admit"/"drain" buttons) honours the same atomic-admission contract as
# Tick: a failing Forwarder.dispatch must roll the claim back so the
# staged rows survive, and the adapter must receive a freshly regenerated
# active_job_id (not the staged payload's job_id).
class ManualAdmissionTest < Minitest::Test
  class ManualJob < ActiveJob::Base
    include DispatchPolicy::JobExtension
    def perform(*); end
  end

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
    truncate_tables!

    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build("manual_test") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 100, per: 60
    end
    DispatchPolicy.registry.register(policy)
    ManualJob.dispatch_policy_name = "manual_test"
  end

  def teardown
    DispatchPolicy.reset_registry!
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )
  end

  def stage_one_job!
    DispatchPolicy::Repository.stage!(
      policy_name:   "manual_test",
      partition_key: "k",
      queue_name:    nil,
      job_class:     "ManualAdmissionTest::ManualJob",
      job_data:      { "job_id" => "ajid-1", "job_class" => "ManualAdmissionTest::ManualJob", "arguments" => [] },
      context:       {},
      priority:      0
    )
  end

  # Fix #1: without the transaction, claim_staged_jobs! would DELETE the
  # staged row, the forward would raise, and the row would be lost.
  def test_forward_failure_rolls_back_the_force_admit
    stage_one_job!
    assert_equal 1, DispatchPolicy::StagedJob.count
    assert_equal 1, DispatchPolicy::Partition.first.pending_count

    forwarder = DispatchPolicy::Forwarder.singleton_class
    original  = forwarder.instance_method(:dispatch)
    forwarder.define_method(:dispatch) { |_rows| raise "adapter exploded" }

    assert_raises(RuntimeError) do
      DispatchPolicy::ManualAdmission.force!(
        policy_name: "manual_test", partition_key: "k", limit: 5
      )
    end
  ensure
    forwarder.define_method(:dispatch, original) if original

    assert_equal 1, DispatchPolicy::StagedJob.count,
                 "TX rollback must restore the deleted staged_job"
    assert_equal 1, DispatchPolicy::Partition.first.pending_count,
                 "TX rollback must revert the partition pending_count decrement"
  end

  def test_successful_force_admit_commits_and_drains
    stage_one_job!

    forwarded = DispatchPolicy::ManualAdmission.force!(
      policy_name: "manual_test", partition_key: "k", limit: 5
    )

    assert_equal 1, forwarded
    assert_equal 0, DispatchPolicy::StagedJob.count, "claimed staged row must be gone"
    assert_equal 0, DispatchPolicy::Partition.first.pending_count
  end

  # M2: force! must pre-insert an inflight row per admitted job, like the
  # Tick does, so the concurrency gate's COUNT(*) sees them immediately
  # instead of under-counting until each job starts performing.
  def test_force_admit_pre_inserts_inflight_rows
    stage_one_job!

    forwarded = DispatchPolicy::ManualAdmission.force!(
      policy_name: "manual_test", partition_key: "k", limit: 5
    )

    assert_equal 1, forwarded
    assert_equal 1,
                 DispatchPolicy::InflightJob.where(policy_name: "manual_test", partition_key: "k").count,
                 "force! must create an inflight row for the admitted job"
  end

  # L7: future-scheduled jobs aren't claimable now, so force!/drain leaves
  # them staged. The controller reports them as "scheduled for later"
  # (via StagedJob.due) instead of looping "click drain again" forever.
  def test_force_admit_skips_future_scheduled_jobs
    stage_one_job! # due now
    DispatchPolicy::Repository.stage!(
      policy_name:   "manual_test",
      partition_key: "k",
      queue_name:    nil,
      job_class:     "ManualAdmissionTest::ManualJob",
      job_data:      { "job_id" => "future-1", "job_class" => "ManualAdmissionTest::ManualJob", "arguments" => [] },
      context:       {},
      scheduled_at:  Time.now.utc + 3600,
      priority:      0
    )

    forwarded = DispatchPolicy::ManualAdmission.force!(
      policy_name: "manual_test", partition_key: "k", limit: 100
    )

    assert_equal 1, forwarded, "only the due job is claimable"
    remaining = DispatchPolicy::StagedJob.for_partition("manual_test", "k")
    assert_equal 1, remaining.count, "the future-scheduled job stays staged"
    assert_equal 0, remaining.due.count, "and it is not due yet"
  end

  def test_empty_partition_forwards_zero_without_error
    assert_equal 0, DispatchPolicy::ManualAdmission.force!(
      policy_name: "manual_test", partition_key: "k", limit: 5
    )
  end

  def test_non_positive_limit_is_a_noop
    stage_one_job!
    assert_equal 0, DispatchPolicy::ManualAdmission.force!(
      policy_name: "manual_test", partition_key: "k", limit: 0
    )
    assert_equal 1, DispatchPolicy::StagedJob.count, "limit <= 0 must not claim anything"
  end

  # Fix #2: the adapter row must use a fresh UUID, not the staged payload's
  # job_id, or good_job/solid_queue raise RecordNotUnique against a residual
  # row from a previous admission.
  def test_force_admit_regenerates_active_job_id
    stage_one_job!
    staged_aj_id = DispatchPolicy::StagedJob.first.job_data["job_id"]
    assert_equal "ajid-1", staged_aj_id

    received_ids = []
    ManualJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue, :enqueue)
    ManualJob.queue_adapter.singleton_class.define_method(:enqueue) do |job|
      received_ids << job.job_id
      __orig_enqueue(job)
    end

    begin
      DispatchPolicy::ManualAdmission.force!(
        policy_name: "manual_test", partition_key: "k", limit: 5
      )
    ensure
      ManualJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue)
    end

    assert_equal 1, received_ids.size, "adapter must receive exactly one enqueue"
    refute_equal staged_aj_id, received_ids.first,
                 "force-admit must hand the adapter a fresh active_job_id"
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/,
                 received_ids.first, "regenerated id must be a UUID")
  end
end
