# frozen_string_literal: true

require_relative "../test_helper"
require "timeout"
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
    errors      = []
    grabbed_lo  = Queue.new
    clicker_pid = Queue.new

    enqueuer = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do |conn|
        conn.transaction do
          lock_row(conn, KEY_LO)
          grabbed_lo << true
          wait_until_blocked!(conn, clicker_pid.pop)
          lock_row(conn, KEY_HI)
        end
      end
    rescue StandardError => e
      errors << ["enqueue", e.class.name]
    end

    grabbed_lo.pop

    clicker = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do |conn|
        clicker_pid << conn.select_value("SELECT pg_backend_pid()").to_i
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

  # The two writes cannot share a transaction — the flip slices on purpose
  # — and without one they are no longer atomic against a CONCURRENT click.
  # A resume that clears the flag while a pause is still walking its slices
  # leaves `paused = false` with every partition still marked paused:
  # nothing admits, the dashboard says the policy is running, and nothing
  # heals it (`upsert_partition!` never writes `status`, and the sweeper
  # needs a `pending_count` of 0 that an unclaimable partition never
  # reaches). Measured at 5 corrupt runs in 6 before the advisory lock.
  def test_a_second_click_is_refused_while_one_is_still_running
    held    = Queue.new
    release = Queue.new

    holder = Thread.new do
      ActiveRecord::Base.connection_pool.with_connection do
        # `with_policy_pause_lock` does NOT yield when it cannot take the
        # lock, so the block alone can never signal. Reporting the refusal
        # is what keeps this test from hanging on `held.pop` forever when
        # some other session on this database is already holding it — and
        # a hung run leaves ITS backend holding the session lock, so every
        # later run against that database hangs here too. That is a test
        # that poisons the database it failed in.
        got = DispatchPolicy::Repository.with_policy_pause_lock(policy_name: POLICY) do
          held << :acquired
          release.pop
        end
        held << :refused unless got
      end
    rescue StandardError => e
      held << e
    end

    outcome = await(held)
    raise outcome if outcome.is_a?(StandardError)
    assert_equal :acquired, outcome,
                 "the pause lock for #{POLICY} was already held on this database — a leaked " \
                 "session lock from an earlier hung run. Clear it with: SELECT " \
                 "pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database()"

    # BOTH actions, because both write the same two rows and either one
    # racing the other produces the wedge — a fix applied to one of them is
    # no fix at all.
    %i[pause resume].each do |action|
      redirects = run_action(action)
      assert_equal DispatchPolicy::PoliciesController::BUSY_NOTICE, redirects.last[:alert],
                   "#{action}: the operator has to be told the click did nothing"
    end

    refute DispatchPolicy::PolicySetting.for_policy(POLICY).pick(:paused),
           "a refused click must not write half of a pause"
    assert_equal 0, DispatchPolicy::Partition.for_policy(POLICY).where(status: "paused").count
  ensure
    release << true
    holder&.join(5)
    holder&.kill
  end

  # The other half, and the one that turns a lock into an outage if it is
  # wrong: the lock is held on the CONNECTION, so a click that forgets to
  # release it refuses every later click on that connection for the life of
  # the process.
  def test_the_lock_is_released_so_the_next_click_works
    2.times do
      assert_equal "Policy paused.", run_action(:pause).last[:notice]
      assert_equal "Policy resumed.", run_action(:resume).last[:notice]
    end
    refute DispatchPolicy::PolicySetting.for_policy(POLICY).pick(:paused)
  end

  private

  # Queue#pop(timeout:) is Ruby 3.2 and the gemspec floor is 3.1. A bare
  # pop here is a hang, not a failure, and a hang leaves a backend holding
  # a session-level advisory lock that makes every later run hang too.
  def await(queue)
    Timeout.timeout(15) { queue.pop }
  rescue Timeout::Error
    flunk "the lock holder never reported: neither acquired nor refused"
  end

  def ordering_step?(name)
    %w[set_policy_paused lock_partitions_for_status].include?(name)
  end

  # Returns the redirects the action issued, so a test can read the flash
  # it set — a refused click is only visible there.
  def run_action(action)
    controller = DispatchPolicy::PoliciesController.new
    controller.instance_variable_set(:@policy_name, POLICY)
    redirects = []
    controller.define_singleton_method(:policy_path) { |*| "/dispatch_policy/policies/x" }
    controller.define_singleton_method(:redirect_to) { |*, **kwargs| redirects << kwargs }
    controller.public_send(action)
    redirects
  end

  def lock_row(conn, key)
    conn.exec_query(
      "SELECT 1 FROM dispatch_policy_partitions WHERE policy_name = $1 AND partition_key = $2 FOR UPDATE",
      "bulk_enqueue_lock", [POLICY, key]
    )
  end

  # Blocks until the CLICK's own backend is waiting on somebody's lock.
  # Deterministic where a sleep is not: too short a sleep lets the click
  # finish before the enqueuer reaches for HI, and the test then passes
  # against the bug.
  #
  # It asks about ONE pid, deliberately. The obvious gates are all
  # cluster-wide and disarm silently: `pg_locks WHERE NOT granted` is
  # satisfied by any backend anywhere waiting on anything — an orphan from
  # a killed run in an unrelated database did exactly that here, and the
  # test passed against the bug eight times out of eight. Narrowing it to
  # this database and a query mentioning the partitions table is not
  # enough either: that matches on TEXT, so a developer's psql session
  # blocked on anything, whose statement happens to name the table, re-arms
  # the same failure. `pg_blocking_pids` names who is blocking THIS
  # backend, and nothing else in the cluster can satisfy it.
  def wait_until_blocked!(conn, pid)
    deadline = Time.now + 10
    loop do
      blocked = conn.select_value("SELECT cardinality(pg_blocking_pids(#{Integer(pid)})) > 0")
      break if blocked == true || blocked == "t"
      raise "the click never blocked on a partition row lock" if Time.now > deadline

      sleep 0.02
    end
  end

  def lock_row(conn, key)
    conn.exec_query(
      "SELECT 1 FROM dispatch_policy_partitions WHERE policy_name = $1 AND partition_key = $2 FOR UPDATE",
      "bulk_enqueue_lock", [POLICY, key]
    )
  end

  # Blocks until the CLICK is waiting on a lock. Deterministic where a
  # sleep is not: too short a sleep lets the click finish before the
  # enqueuer reaches for HI, and the test then passes against the bug.
  #
  # The obvious gate — `SELECT count(*) FROM pg_locks WHERE NOT granted` —
  # is worse than a sleep, because it disarms silently. `pg_locks` is
  # CLUSTER-wide: any backend anywhere, in any database, waiting on any
  # lock satisfies it on the first poll. A leftover backend from another
  # run, or a second test process, and this returns immediately while the
  # click is nowhere near a lock. So: this database, not our own backend,
  # actually waiting on a Lock, and waiting inside a statement against the
  # table in question.
  def wait_for_the_click_to_block!
    deadline = Time.now + 10
    loop do
      blocked = ActiveRecord::Base.connection_pool.with_connection do |c|
        # This poll runs inside the enqueuer's open transaction, and
        # Postgres caches the backend-status snapshot behind
        # pg_stat_activity for the life of a transaction — without this the
        # view answers with the state from before the click existed, every
        # time, until the deadline. (pg_locks reads the lock manager live,
        # which is why the cluster-wide version did not need it.)
        c.execute("SELECT pg_stat_clear_snapshot()")
        c.select_value(<<~SQL.squish).to_i
          SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database()
            AND pid <> pg_backend_pid()
            AND wait_event_type = 'Lock'
            AND query LIKE '%#{DispatchPolicy::Repository::PARTITIONS_TABLE}%'
        SQL
      end
      break if blocked.positive?
      raise "the click never blocked on a partition row lock" if Time.now > deadline

      sleep 0.02
    end
  end
end
