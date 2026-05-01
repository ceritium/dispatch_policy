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
end
