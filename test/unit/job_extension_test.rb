# frozen_string_literal: true

require_relative "../test_helper"

class JobExtensionTest < Minitest::Test
  class Recorder
    @rows = []
    class << self; attr_accessor :rows; end
  end

  def setup
    super
    Recorder.rows = []

    # Stub Repository.stage! so this test doesn't need a database.
    DispatchPolicy::Repository.singleton_class.send(:alias_method, :__orig_stage, :stage!) unless DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_stage)
    DispatchPolicy::Repository.singleton_class.define_method(:stage!) { |**kw| Recorder.rows << kw; true }

    # Build a job class that uses the macro
    klass = Class.new(ActiveJob::Base) do
      include DispatchPolicy::JobExtension

      dispatch_policy :testpolicy do
        context ->(args) { { key: args.first } }
        gate :throttle, rate: 5, per: 60, partition_by: ->(c) { c[:key] }
      end

      def perform(_arg); end
    end
    Object.const_set(:JobExtensionTestJob, klass) unless Object.const_defined?(:JobExtensionTestJob)
  end

  def teardown
    if DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_stage)
      DispatchPolicy::Repository.singleton_class.send(:alias_method, :stage!, :__orig_stage)
    end
    Object.send(:remove_const, :JobExtensionTestJob) if Object.const_defined?(:JobExtensionTestJob)
  end

  def test_perform_later_routes_through_stage
    JobExtensionTestJob.perform_later("hello")

    assert_equal 1, Recorder.rows.size
    row = Recorder.rows.first
    assert_equal "testpolicy", row[:policy_name]
    assert_equal "throttle=hello", row[:partition_key]
    assert_equal "JobExtensionTestJob", row[:job_class]
    assert_equal({"key" => "hello"}, row[:context])
  end

  def test_bypass_block_does_not_stage
    DispatchPolicy::Bypass.with do
      JobExtensionTestJob.perform_later("inside-bypass")
    end
    assert_empty Recorder.rows
  end

  def test_perform_all_later_routes_jobs_with_policy_through_bulk_stage
    # Stub stage_many! to capture the bulk-insert call.
    repo = DispatchPolicy::Repository.singleton_class
    repo.send(:alias_method, :__orig_stage_many, :stage_many!) unless repo.method_defined?(:__orig_stage_many)
    captured = []
    repo.define_method(:stage_many!) { |rows| captured.replace(rows); rows.size }

    # Install BulkEnqueue patch (railtie installs in real apps; in tests we
    # do it manually to avoid relying on Rails::Railtie boot order).
    unless ActiveJob.singleton_class.include?(DispatchPolicy::JobExtension::BulkEnqueue)
      ActiveJob.singleton_class.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
    end

    jobs = [JobExtensionTestJob.new("a"), JobExtensionTestJob.new("b"), JobExtensionTestJob.new("c")]
    ActiveJob.perform_all_later(jobs)

    assert_equal 3, captured.size
    keys = captured.map { |r| r[:partition_key] }
    assert_equal %w[throttle=a throttle=b throttle=c], keys
    assert captured.all? { |r| r[:policy_name] == "testpolicy" }
  ensure
    repo.send(:alias_method, :stage_many!, :__orig_stage_many) if repo.method_defined?(:__orig_stage_many)
  end
end
