# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/policy_setting"
require "action_controller"
require_relative "../../app/controllers/dispatch_policy/application_controller"
require_relative "../../app/controllers/dispatch_policy/policies_controller"

# The pause/resume button writes every partition row of a policy. It used
# to do that with one `Partition.for_policy(name).update_all(...)` inside a
# transaction, which has no lock order of its own: a seq scan locks in heap
# order, an index scan in the DATABASE's collation, and neither is the byte
# order `stage_many!` sorts by. Against one ordinary bulk-enqueuing process
# that is A1's deadlock — measured at 5 in 12 clicks — landing in the worst
# possible place: the click happens during the load that made someone want
# to pause, Postgres kills the controller's transaction, so the policy is
# NOT paused, the tick keeps admitting, and the request 500s with nothing
# saying so.
class PauseLockOrderTest < DispatchPolicy::IntegrationTest
  POLICY = "pause_order"
  # Byte order is LO < HI; the rows are INSERTED the other way round, so
  # heap order (what a seq scan takes its locks in) is the reverse.
  KEY_LO = "acct:A"
  KEY_HI = "acct:Z"

  def setup
    super
    [KEY_HI, KEY_LO].each do |key|
      DispatchPolicy::Repository.upsert_partition!(
        policy_name: POLICY, partition_key: key, queue_name: nil,
        context: {}, delta_pending: 1
      )
    end
  end

  def capture_sql
    seen = []
    sub = ActiveSupport::Notifications.subscribe("sql.active_record") { |*, payload| seen << payload }
    yield
    seen
  ensure
    ActiveSupport::Notifications.unsubscribe(sub)
  end

  # The behavioural half: a real deadlock, forced rather than raced.
  #
  # A second connection plays the bulk enqueue and takes its locks the way
  # `stage_many!` does — byte-ascending, LO then HI. It grabs LO, waits
  # until the click is provably blocked on a row lock (pg_locks, not a
  # sleep), and only then reaches for HI. If the click locked in heap order
  # it is sitting on HI waiting for LO, and the two deadlock; if it locks
  # byte-ascending it is waiting on LO holding nothing, so HI is free and
  # both finish.
  def test_pausing_under_a_concurrent_bulk_enqueue_does_not_deadlock
    errors = []
    grabbed_lo = Queue.new

    enqueuer = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do |conn|
        conn.transaction do
          lock_row(conn, KEY_LO)
          grabbed_lo << true
          wait_for_a_blocked_lock_request!
          lock_row(conn, KEY_HI)
        end
      end
    rescue StandardError => e
      errors << ["enqueue", e.class.name]
    end

    grabbed_lo.pop

    clicker = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        DispatchPolicy::Repository.set_policy_paused!(policy_name: POLICY, paused: true)
        DispatchPolicy::Repository.set_partitions_status!(policy_name: POLICY, status: "paused")
      end
    rescue StandardError => e
      errors << ["click", e.class.name]
    end

    [enqueuer, clicker].each { |t| t.join(20) }

    assert_empty errors,
                 "a pause taken during a bulk enqueue must not deadlock — a killed " \
                 "transaction here means the policy is NOT paused while the tick keeps admitting"
    assert DispatchPolicy::PolicySetting.for_policy(POLICY).pick(:paused),
           "the pause flag is the source of truth the tick reads"
    assert_equal 2, DispatchPolicy::Partition.for_policy(POLICY).where(status: "paused").count
  end

  # The behavioural test above runs on whatever collation the local
  # database has, so it cannot tell `ORDER BY` from `ORDER BY … COLLATE
  # "C"` — on an en_US.UTF-8 database (RDS, Heroku, the official postgres
  # image, Debian/Ubuntu) those two disagree on ordinary keys and the
  # deadlock comes straight back. Pin the statement.
  def test_the_partition_locks_are_taken_in_canonical_order_before_the_update
    seen = capture_sql do
      DispatchPolicy::Repository.set_partitions_status!(policy_name: POLICY, status: "paused")
    end

    names = seen.map { |p| p[:name] }
    lock  = names.index("lock_partitions_for_status")
    upd   = names.index("set_partitions_status")

    refute_nil lock, "the status flip must take its row locks explicitly, not leave the order to the planner"
    refute_nil upd
    assert_operator lock, :<, upd, "locking after the UPDATE would be no ordering at all"

    assert_match(/ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"/,
                 seen[lock][:sql],
                 "without COLLATE \"C\" the lock order is the database's collation, " \
                 "which is not the order stage_many! sorts by")

    keys = seen[lock][:binds].map { |b| b.respond_to?(:value) ? b.value : b }
                             .reject { |v| v == POLICY }
    assert_equal [KEY_LO, KEY_HI], keys, "byte-ascending, like stage_many!"
  end

  # One bind per partition and one transaction over all of them would hit
  # Postgres' parameter ceiling and hold every row lock of the policy for
  # the whole flip — blocking each perform_later for it behind an
  # operator's click, which is A1's lock convoy with extra steps.
  def test_the_status_flip_is_sliced
    batch = DispatchPolicy::Repository::PARTITION_STATUS_BATCH
    (1..(batch + 5)).each do |i|
      DispatchPolicy::Repository.upsert_partition!(
        policy_name: POLICY, partition_key: format("k%05d", i), queue_name: nil,
        context: {}, delta_pending: 1
      )
    end

    seen = capture_sql do
      DispatchPolicy::Repository.set_partitions_status!(policy_name: POLICY, status: "paused")
    end

    shape = seen.filter_map do |p|
      next p[:sql].strip[0, 6] if p[:name] == "TRANSACTION"

      "LOCK" if p[:name] == "lock_partitions_for_status"
    end
    assert_equal %w[BEGIN LOCK COMMIT BEGIN LOCK COMMIT], shape,
                 "one transaction per slice, so a large policy never holds every " \
                 "partition's row lock at once"
    assert_equal batch + 7, DispatchPolicy::Partition.for_policy(POLICY).where(status: "paused").count
  end

  # Round 3's lesson: the fix has to be reachable from the button. Driving
  # the action itself is what pins that the controller stopped using the
  # unordered `update_all` — and the ORDER of its two writes, which is what
  # makes the (now sliced, no longer all-or-nothing) flip safe: the flag
  # that actually stops admission is written first on the way in and last
  # on the way out, so every partial state is "more paused", never "the UI
  # says paused while the tick admits".
  def test_the_pause_action_writes_the_flag_first_through_the_ordered_flip
    seen = capture_sql { run_action(:pause) }

    assert_equal %w[set_policy_paused lock_partitions_for_status],
                 seen.map { |p| p[:name] }.select { |n| ordering_step?(n) }.uniq,
                 "pause: the source-of-truth flag first, then the partition statuses"
    refute_includes seen.map { |p| p[:name] }, "SQL",
                    "an anonymous update_all is the unordered write this replaced"
    assert DispatchPolicy::PolicySetting.for_policy(POLICY).pick(:paused)
    assert_equal 2, DispatchPolicy::Partition.for_policy(POLICY).where(status: "paused").count
  end

  def test_the_resume_action_clears_the_flag_last
    run_action(:pause)
    seen = capture_sql { run_action(:resume) }

    assert_equal %w[lock_partitions_for_status set_policy_paused],
                 seen.map { |p| p[:name] }.select { |n| ordering_step?(n) }.uniq,
                 "resume: statuses first, so a half-done resume stays paused"
    refute DispatchPolicy::PolicySetting.for_policy(POLICY).pick(:paused)
    assert_equal 2, DispatchPolicy::Partition.for_policy(POLICY).where(status: "active").count
  end

  private

  def ordering_step?(name)
    %w[set_policy_paused lock_partitions_for_status].include?(name)
  end

  def run_action(action)
    controller = DispatchPolicy::PoliciesController.new
    controller.instance_variable_set(:@policy_name, POLICY)
    def controller.policy_path(*) = "/dispatch_policy/policies/x"
    def controller.redirect_to(*, **) = nil
    controller.public_send(action)
  end

  def lock_row(conn, key)
    conn.exec_query(
      "SELECT 1 FROM dispatch_policy_partitions WHERE policy_name = $1 AND partition_key = $2 FOR UPDATE",
      "bulk_enqueue_lock", [POLICY, key]
    )
  end

  # Blocks until some backend is waiting on a row lock. Deterministic
  # where a sleep is not: too short a sleep lets the click finish before
  # the enqueuer reaches for HI, and the test then passes against the bug.
  def wait_for_a_blocked_lock_request!
    deadline = Time.now + 10
    loop do
      blocked = ActiveRecord::Base.connection_pool.with_connection do |c|
        c.select_value("SELECT count(*) FROM pg_locks WHERE NOT granted").to_i
      end
      break if blocked.positive?
      raise "no lock request ever blocked" if Time.now > deadline

      sleep 0.02
    end
  end
end
