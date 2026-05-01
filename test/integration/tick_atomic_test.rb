# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# Verifies the atomic admission contract: when Forwarder.dispatch raises
# from inside Tick's transaction, the entire admission is rolled back —
# staged_jobs return, inflight rows disappear, partition counters revert.
class TickAtomicAdmissionTest < Minitest::Test
  class TestTickJob < ActiveJob::Base
    def perform(_); end
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
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build("atomic_test") do
        context ->(_args) { {} }
        gate :throttle, rate: 100, per: 60, partition_by: ->(_c) { "k" }
      end
    )
    Object.const_set(:AtomicTestJob, TestTickJob) unless Object.const_defined?(:AtomicTestJob)
  end

  def teardown
    DispatchPolicy.reset_registry!
    Object.send(:remove_const, :AtomicTestJob) if Object.const_defined?(:AtomicTestJob)
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )
  end

  def stage_one_job!
    DispatchPolicy::Repository.stage!(
      policy_name:   "atomic_test",
      partition_key: "k",
      queue_name:    nil,
      job_class:     "TickAtomicAdmissionTest::TestTickJob",
      job_data:      { "job_id" => "ajid-1", "job_class" => "TickAtomicAdmissionTest::TestTickJob", "arguments" => [] },
      context:       {},
      priority:      0
    )
  end

  def test_forward_failure_rolls_back_the_whole_admission
    stage_one_job!
    assert_equal 1, DispatchPolicy::StagedJob.count
    assert_equal 1, DispatchPolicy::Partition.first.pending_count

    # Stub Forwarder.dispatch to raise after a row is claimed.
    forwarder = DispatchPolicy::Forwarder.singleton_class
    original  = forwarder.instance_method(:dispatch)
    forwarder.define_method(:dispatch) { |_rows| raise "adapter exploded" }

    begin
      DispatchPolicy::Tick.run(policy_name: "atomic_test")
    ensure
      forwarder.define_method(:dispatch, original)
    end

    # Everything must be unchanged: the staged row is back (TX rolled back
    # the DELETE), no inflight row was leaked, and the partition counter
    # reflects the original pending count.
    assert_equal 1, DispatchPolicy::StagedJob.count,
                 "TX rollback must restore the deleted staged_job"
    assert_equal 0, DispatchPolicy::InflightJob.count,
                 "TX rollback must drop the pre-inserted inflight row"
    assert_equal 1, DispatchPolicy::Partition.first.pending_count,
                 "TX rollback must revert the partition pending_count decrement"
  end

  def test_successful_admission_commits_everything
    stage_one_job!

    received = []
    AtomicTestJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue, :enqueue)
    AtomicTestJob.queue_adapter.singleton_class.define_method(:enqueue) do |job|
      received << job.class.name
      __orig_enqueue(job)
    end

    begin
      DispatchPolicy::Tick.run(policy_name: "atomic_test")
    ensure
      AtomicTestJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue)
    end

    assert_equal 0, DispatchPolicy::StagedJob.count, "claimed staged row must be gone"
    assert_equal 1, DispatchPolicy::InflightJob.count, "inflight row must persist after commit"
    assert_equal 0, DispatchPolicy::Partition.first.pending_count
    assert_equal ["TickAtomicAdmissionTest::TestTickJob"], received
  end
end
