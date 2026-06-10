# frozen_string_literal: true

require_relative "../test_helper"

# M3: a job discarded before its perform callbacks run (e.g. discard_on
# ActiveJob::DeserializationError) never reaches track's `ensure`, so the
# Tick's pre-inserted inflight row would orphan. handle_discard, wired to
# the discard.active_job notification, reaps it.
class InflightTrackerDiscardTest < Minitest::Test
  def with_delete_stub
    repo = DispatchPolicy::Repository.singleton_class
    original = repo.instance_method(:delete_inflight!)
    calls = []
    repo.define_method(:delete_inflight!) { |active_job_id:| calls << active_job_id }
    yield calls
  ensure
    repo.define_method(:delete_inflight!, original)
  end

  def policy_job_instance
    klass = Class.new(ActiveJob::Base) do
      include DispatchPolicy::JobExtension
      self.dispatch_policy_name = "some_policy"
      def perform(*); end
    end
    klass.new
  end

  def plain_job_instance
    Class.new(ActiveJob::Base) { def perform(*); end }.new
  end

  def test_handle_discard_deletes_inflight_row_for_policy_job
    with_delete_stub do |calls|
      job = policy_job_instance
      DispatchPolicy::InflightTracker.handle_discard(job)
      assert_equal [job.job_id], calls,
                   "a discarded policy job's inflight row must be deleted by its active_job_id"
    end
  end

  def test_handle_discard_ignores_jobs_without_a_policy
    with_delete_stub do |calls|
      DispatchPolicy::InflightTracker.handle_discard(plain_job_instance)
      assert_empty calls, "non-policy jobs have no inflight row to reap"
    end
  end

  def test_handle_discard_tolerates_nil
    with_delete_stub do |calls|
      DispatchPolicy::InflightTracker.handle_discard(nil)
      assert_empty calls
    end
  end
end
