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
  # `connected_to` is block-scoped, so once Repository.with_connection
  # returns, `current_role` is back to :writing and
  # ActiveRecord::Base.connection_pool resolves to the WRITING pool —
  # while the lease to hand back belongs to the role's pool, where the
  # inflight row lives. Releasing outside the block is the same leak with
  # an extra step, and it only shows up on a multi-database install.
  def test_a_beat_releases_its_connection_inside_the_configured_role
    DispatchPolicy.config.database_role = :queue

    depth       = 0
    released_at = nil
    connected_to = lambda do |role:, &blk|
      depth += 1
      blk.call
    ensure
      depth -= 1
    end
    pool = Object.new
    pool.define_singleton_method(:release_connection) { released_at = depth }

    DispatchPolicy::Repository.stub(:heartbeat_inflight!, nil) do
      ActiveRecord::Base.stub(:connected_to, connected_to) do
        ActiveRecord::Base.stub(:connection_pool, pool) do
          DispatchPolicy::InflightTracker.beat!("ajid-role")
        end
      end
    end

    assert_equal 1, released_at,
                 "released outside the role block, i.e. against the wrong pool"
  ensure
    DispatchPolicy.reset_config!
  end

  # The rule the railtie's perform.active_job subscription applies. It
  # lives in a method rather than in the initializer block because a
  # subscription body is unreachable from the suite: inverting it there
  # left 252 runs green while completely undoing the fix.
  def test_handle_failed_perform_only_reaps_when_the_perform_failed
    reaped = []
    event  = Struct.new(:payload)

    DispatchPolicy::InflightTracker.stub(:handle_discard, ->(job) { reaped << job }) do
      DispatchPolicy::InflightTracker.handle_failed_perform(event.new({ job: :ok }))
      assert_empty reaped, "a successful perform has already been cleaned up by track's ensure"

      DispatchPolicy::InflightTracker.handle_failed_perform(
        event.new({ job: :dead, exception: ["StandardError", "boom"] })
      )
      assert_equal [:dead], reaped
    end
  end
end
