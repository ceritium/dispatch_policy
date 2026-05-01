# frozen_string_literal: true

require_relative "../test_helper"

class ForwarderTest < Minitest::Test
  class GoodJob < ActiveJob::Base
    def perform(_); end
  end

  class BoomJob < ActiveJob::Base
    def perform(_); end
  end

  def setup
    super
    Object.const_set(:FwdGoodJob, GoodJob) unless Object.const_defined?(:FwdGoodJob)
    Object.const_set(:FwdBoomJob, BoomJob) unless Object.const_defined?(:FwdBoomJob)
  end

  def teardown
    Object.send(:remove_const, :FwdGoodJob) if Object.const_defined?(:FwdGoodJob)
    Object.send(:remove_const, :FwdBoomJob) if Object.const_defined?(:FwdBoomJob)
  end

  def staged_row(klass, ajid)
    {
      "policy_name"   => "p",
      "partition_key" => "k",
      "queue_name"    => nil,
      "job_class"     => klass.name,
      "job_data"      => { "job_class" => klass.name, "job_id" => ajid, "arguments" => [{}] },
      "context"       => {},
      "scheduled_at"  => nil,
      "priority"      => 0,
      "enqueued_at"   => Time.now
    }
  end

  def test_dispatch_forwards_each_row_under_bypass
    received = []
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue, :enqueue)
    FwdGoodJob.queue_adapter.singleton_class.define_method(:enqueue) do |job|
      received << [DispatchPolicy::Bypass.active?, job.class.name]
      __orig_enqueue(job)
    end

    rows = [staged_row(FwdGoodJob, "abc")]
    DispatchPolicy::Forwarder.dispatch(rows)

    assert_equal [[true, "ForwarderTest::GoodJob"]], received
  ensure
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue) if FwdGoodJob.queue_adapter.singleton_class.method_defined?(:__orig_enqueue)
  end

  # The new contract is "all or nothing": any per-row failure propagates so
  # the surrounding admission TX can roll back. There is no per-row failure
  # return value anymore — compensation (unclaim, delete inflight) is gone
  # because TX rollback handles it for free.
  def test_dispatch_propagates_per_row_failures_to_caller
    FwdBoomJob.queue_adapter.singleton_class.define_method(:enqueue) { |_job| raise "boom" }

    rows = [staged_row(FwdBoomJob, "ajid-99")]
    err = assert_raises(RuntimeError) { DispatchPolicy::Forwarder.dispatch(rows) }
    assert_equal "boom", err.message
  ensure
    FwdBoomJob.queue_adapter.singleton_class.send(:remove_method, :enqueue) rescue nil
  end

  def test_dispatch_no_op_on_empty
    DispatchPolicy::Forwarder.dispatch([])
  end
end
