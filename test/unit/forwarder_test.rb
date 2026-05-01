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

    @inflight_deletes = []
    @unclaimed        = []

    repo_singleton = DispatchPolicy::Repository.singleton_class
    @originals = {}
    %i[delete_inflight! unclaim!].each do |m|
      @originals[m] = repo_singleton.instance_method(m)
    end
    repo_singleton.define_method(:delete_inflight!) { |active_job_id:| ForwarderTest.collected_deletes << active_job_id }
    repo_singleton.define_method(:unclaim!) { |rows| ForwarderTest.collected_unclaims.concat(rows) }
  end

  def teardown
    @originals&.each { |m, mi| DispatchPolicy::Repository.singleton_class.define_method(m, mi) }
    Object.send(:remove_const, :FwdGoodJob) if Object.const_defined?(:FwdGoodJob)
    Object.send(:remove_const, :FwdBoomJob) if Object.const_defined?(:FwdBoomJob)
    self.class.collected_deletes.clear
    self.class.collected_unclaims.clear
  end

  class << self
    def collected_deletes; @cd ||= []; end
    def collected_unclaims; @cu ||= []; end
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
    failures = DispatchPolicy::Forwarder.dispatch(rows)

    assert_empty failures
    assert_equal [[true, "ForwarderTest::GoodJob"]], received
  ensure
    FwdGoodJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue) if FwdGoodJob.queue_adapter.singleton_class.method_defined?(:__orig_enqueue)
  end

  def test_failed_forward_unclaims_and_deletes_preinserted_inflight
    FwdBoomJob.queue_adapter.singleton_class.define_method(:enqueue) { |_job| raise "boom" }

    rows = [staged_row(FwdBoomJob, "ajid-99")]
    failures = DispatchPolicy::Forwarder.dispatch(rows, preinserted_inflight_ids: ["ajid-99"])

    assert_equal 1, failures.size
    assert_equal "boom", failures.first.error.message
    assert_equal ["ajid-99"], self.class.collected_deletes,
                 "preinserted inflight rows must be deleted on forward failure to keep concurrency budget accurate"
    assert_equal 1, self.class.collected_unclaims.size
  ensure
    FwdBoomJob.queue_adapter.singleton_class.send(:remove_method, :enqueue) rescue nil
  end

  def test_failed_forward_without_preinsert_does_not_delete_inflight
    FwdBoomJob.queue_adapter.singleton_class.define_method(:enqueue) { |_job| raise "boom" }

    rows = [staged_row(FwdBoomJob, "ajid-99")]
    DispatchPolicy::Forwarder.dispatch(rows, preinserted_inflight_ids: [])

    assert_empty self.class.collected_deletes
  ensure
    FwdBoomJob.queue_adapter.singleton_class.send(:remove_method, :enqueue) rescue nil
  end
end
