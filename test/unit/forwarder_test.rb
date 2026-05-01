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

  def staged_row(klass, ajid, scheduled_at: nil)
    {
      "policy_name"   => "p",
      "partition_key" => "k",
      "queue_name"    => nil,
      "job_class"     => klass.name,
      "job_data"      => { "job_class" => klass.name, "job_id" => ajid, "arguments" => [{}] },
      "context"       => {},
      "scheduled_at"  => scheduled_at,
      "priority"      => 0,
      "enqueued_at"   => Time.now
    }
  end

  def test_dispatch_uses_perform_all_later_for_immediate_rows
    received_bulk = []
    received_single = []
    original_bulk = ActiveJob.singleton_class.instance_method(:perform_all_later)
    ActiveJob.singleton_class.define_method(:perform_all_later) do |jobs|
      received_bulk << [DispatchPolicy::Bypass.active?, jobs.map(&:class).map(&:name)]
      jobs.each { |j| j.successfully_enqueued = true if j.respond_to?(:successfully_enqueued=) }
      jobs
    end
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue, :enqueue)
    FwdGoodJob.queue_adapter.singleton_class.define_method(:enqueue) do |job|
      received_single << job.class.name
      __orig_enqueue(job)
    end

    rows = [staged_row(FwdGoodJob, "a"), staged_row(FwdGoodJob, "b")]
    DispatchPolicy::Forwarder.dispatch(rows)

    assert_equal 1, received_bulk.size, "all immediate rows must go through one perform_all_later call"
    assert_equal [true, ["ForwarderTest::GoodJob", "ForwarderTest::GoodJob"]], received_bulk.first
    assert_empty received_single, "no per-row enqueue should be called for immediate rows"
  ensure
    ActiveJob.singleton_class.define_method(:perform_all_later, original_bulk)
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue) if FwdGoodJob.queue_adapter.singleton_class.method_defined?(:__orig_enqueue)
  end

  def test_dispatch_uses_per_row_enqueue_for_scheduled_rows
    received_single = []
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue_at, :enqueue_at)
    FwdGoodJob.queue_adapter.singleton_class.define_method(:enqueue_at) do |job, ts|
      received_single << [DispatchPolicy::Bypass.active?, job.class.name, ts]
      __orig_enqueue_at(job, ts)
    end

    future = Time.now + 60
    rows = [staged_row(FwdGoodJob, "a", scheduled_at: future)]
    DispatchPolicy::Forwarder.dispatch(rows)

    assert_equal 1, received_single.size
    assert_equal true, received_single.first[0]
    assert_equal "ForwarderTest::GoodJob", received_single.first[1]
  ensure
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:enqueue_at, :__orig_enqueue_at) if FwdGoodJob.queue_adapter.singleton_class.method_defined?(:__orig_enqueue_at)
  end

  # The new contract is "all or nothing": any per-row failure propagates so
  # the surrounding admission TX can roll back. There is no per-row failure
  # return value anymore — compensation (unclaim, delete inflight) is gone
  # because TX rollback handles it for free.
  def test_dispatch_propagates_bulk_raise_to_caller
    original_bulk = ActiveJob.singleton_class.instance_method(:perform_all_later)
    ActiveJob.singleton_class.define_method(:perform_all_later) { |_jobs| raise "boom" }

    rows = [staged_row(FwdBoomJob, "ajid-99")]
    err = assert_raises(RuntimeError) { DispatchPolicy::Forwarder.dispatch(rows) }
    assert_equal "boom", err.message
  ensure
    ActiveJob.singleton_class.define_method(:perform_all_later, original_bulk)
  end

  def test_dispatch_raises_when_bulk_soft_fails_some_jobs
    original_bulk = ActiveJob.singleton_class.instance_method(:perform_all_later)
    ActiveJob.singleton_class.define_method(:perform_all_later) do |jobs|
      # Simulate an adapter that returns without raising but only enqueued
      # the first job — perform_all_later's documented contract.
      jobs.first.successfully_enqueued = true if jobs.first.respond_to?(:successfully_enqueued=)
      jobs.last.successfully_enqueued  = false if jobs.last.respond_to?(:successfully_enqueued=)
      jobs
    end

    rows = [staged_row(FwdGoodJob, "ok"), staged_row(FwdGoodJob, "nope")]
    err = assert_raises(DispatchPolicy::EnqueueFailed) { DispatchPolicy::Forwarder.dispatch(rows) }
    assert_match(/1\/2/, err.message)
  ensure
    ActiveJob.singleton_class.define_method(:perform_all_later, original_bulk)
  end

  def test_dispatch_no_op_on_empty
    DispatchPolicy::Forwarder.dispatch([])
  end
end
