# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/staged_job"

# `bulk_record_partition_denies!` is ONE statement, and one statement
# gives no lock-ordering guarantee: the planner joins its VALUES list
# against a seq scan, so it takes row locks in heap order — unrelated to
# (policy_name, partition_key), and unrelated to the order of the VALUES
# list, which is why sorting the Ruby array does not help.
#
# `stage_many!` sorts its per-partition upserts precisely so concurrent
# bulk enqueues agree on an order. Against that, the unordered deny
# deadlocks: measured at 16 in 20 seconds from ONE tick loop plus ONE
# process calling perform_all_later, no misconfiguration. Half aborted
# the caller's bulk enqueue mid-batch; the other half killed
# `Tick#flush_denies!`, which only logs — so every denied partition in
# that tick lost its backoff AND its gate_state patch and was
# immediately re-claimable, which is the M4 busy-loop for a whole tick.
#
# Reproducing a deadlock in a test means racing two connections and
# hoping; what makes the fix correct is the ORDER, so that is what this
# pins.
class DenyLockOrderTest < DispatchPolicy::IntegrationTest
  POLICY = "deny_order"

  def setup
    super
    # Keys chosen to discriminate: byte order and en_US.UTF-8 disagree on
    # every pair here, so a bare ORDER BY produces a different order.
    %w[acct:10 acct:1:eu Acme].each do |key|
      DispatchPolicy::Repository.upsert_partition!(
        policy_name: POLICY, partition_key: key, queue_name: nil,
        context: {}, delta_pending: 1
      )
    end
  end

  def capture_sql
    seen = []
    sub = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
      seen << payload
    end
    yield
    seen
  ensure
    ActiveSupport::Notifications.unsubscribe(sub)
  end

  def test_the_partition_locks_are_taken_in_canonical_order_before_the_update
    seen = capture_sql do
      DispatchPolicy::Repository.bulk_record_partition_denies!(
        %w[acct:10 acct:1:eu Acme].map do |key|
          { policy_name: POLICY, partition_key: key,
            gate_state_patch: {}, retry_after: 30 }
        end
      )
    end

    names = seen.map { |p| p[:name] }
    lock  = names.index("lock_partitions_for_deny")
    upd   = names.index("bulk_record_partition_denies")

    refute_nil lock, "the deny must take its row locks explicitly, not leave the order to the planner"
    assert_operator lock, :<, upd, "locking after the UPDATE would be no ordering at all"

    # The binds come from the Ruby `.uniq.sort`, so asserting them alone
    # says nothing about the STATEMENT — deleting the ORDER BY entirely
    # left this file green while the deadlock came back. Pin the SQL.
    assert_match(/ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"/,
                 seen[lock][:sql],
                 "without the ORDER BY the planner locks in heap order; without " \
                 "COLLATE \"C\" it locks in the database's collation, which is not " \
                 "the order stage_many! sorts by")

    keys = seen[lock][:binds].map { |b| b.respond_to?(:value) ? b.value : b }
                             .reject { |v| v == POLICY }
    assert_equal %w[Acme acct:10 acct:1:eu], keys,
                 "the order has to match what stage_many! sorts by, or the two still cross"
  end

  # The ordering must not have changed what the flush actually records.
  def test_the_deny_still_records_backoff_and_patch_for_every_entry
    DispatchPolicy::Repository.bulk_record_partition_denies!([
      { policy_name: POLICY, partition_key: "Acme",
        gate_state_patch: { "throttle" => { "tokens" => 3.0 } }, retry_after: 30 },
      { policy_name: POLICY, partition_key: "acct:10",
        gate_state_patch: {}, retry_after: 60 }
    ])

    first  = DispatchPolicy::Partition.find_by(partition_key: "Acme")
    second = DispatchPolicy::Partition.find_by(partition_key: "acct:10")

    assert_in_delta 3.0, first.gate_state.dig("throttle", "tokens").to_f, 0.01
    refute_nil first.next_eligible_at
    refute_nil second.next_eligible_at
    assert_operator second.next_eligible_at, :>, first.next_eligible_at
  end
  # The quarantine release writes many partition rows in one pass too, so
  # it takes the same lock order — a second statement crossing
  # stage_many! would reintroduce A1 exactly as the deny flush did.
  def test_the_quarantine_release_takes_its_locks_in_the_same_order
    DispatchPolicy::Repository.stage!(
      policy_name: POLICY, partition_key: "acct:10", queue_name: nil,
      job_class: "X", job_data: {}, context: {}
    )
    id = DispatchPolicy::StagedJob.first.id
    DispatchPolicy::Repository.quarantine_staged_jobs!(
      policy_name: POLICY, partition_key: "acct:10", ids: [id], reason: "test"
    )
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_staged_jobs SET failed_at = now() - interval '2 hours'"
    )

    seen = capture_sql do
      DispatchPolicy::Repository.release_aged_quarantines!(policy_name: POLICY, older_than: 60)
    end

    lock = seen.find { |p| p[:name] == "lock_partitions_for_quarantine_release" }
    refute_nil lock, "without an explicit ordered lock this deadlocks against stage_many!"
    assert_match(/ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"/, lock[:sql])
  end
  # The partition sweeper's DELETE writes many partition rows too. A bare
  # `DELETE … WHERE` locks in whatever order the plan produces — an index
  # scan on idx_dp_partitions_scheduled_order tie-breaks equal keys by
  # ctid, i.e. heap order — which is the A1 hazard, and this was the last
  # multi-row writer of the table still carrying it. No deadlock was
  # actually reproduced for this one (0 in a 20s stress run on an isolated
  # database, before and after); it is pinned because the guarantee is
  # meant to be structural, and because Postgres usually picks the sweep as
  # the victim, whose blanket rescue then silently skips the rest of that
  # pass — partition GC, tick-sample GC and adaptive-stat GC.
  def test_the_partition_sweep_takes_its_locks_in_the_same_order
    seen = capture_sql do
      DispatchPolicy::Repository.sweep_inactive_partitions!(cutoff_seconds: 0, policy_name: POLICY)
    end

    sweep = seen.find { |p| p[:name] == "sweep_inactive_partitions" }
    refute_nil sweep
    assert_match(/ORDER BY p.policy_name COLLATE "C", p.partition_key COLLATE "C"/, sweep[:sql],
                 "without it the DELETE locks in heap order and crosses stage_many!")
    assert_match(/FOR UPDATE OF p SKIP LOCKED/, sweep[:sql],
                 "a periodic best-effort sweep must skip rows somebody is writing, not wait on them")
  end

  # …and still deletes what it is supposed to.
  def test_the_partition_sweep_still_collects_a_drained_partition
    DispatchPolicy::Repository.connection.execute(
      "UPDATE dispatch_policy_partitions SET pending_count = 0, " \
      "created_at = now() - interval '2 hours', last_admit_at = NULL"
    )

    DispatchPolicy::Repository.sweep_inactive_partitions!(cutoff_seconds: 60, policy_name: POLICY)

    assert_equal 0, DispatchPolicy::Partition.for_policy(POLICY).count
  end

  # The ordered CTE introduced a failure the single `DELETE … WHERE` could
  # not have: a wrong or incomplete join back to the victims. Dropping
  # `AND d.partition_key = v.partition_key` makes the sweep delete EVERY
  # partition of the policy as soon as ONE of them is collectable —
  # destroying the token buckets and pending counts of partitions that
  # still hold work, which is M11's quota reset with a bigger blast
  # radius. Nothing else in the suite exercises the join.
  def test_the_sweep_deletes_only_the_partitions_it_selected
    DispatchPolicy::Repository.connection.execute(
      "UPDATE dispatch_policy_partitions SET pending_count = 0, " \
      "created_at = now() - interval '2 hours', last_admit_at = NULL " \
      "WHERE partition_key = 'Acme'"
    )

    DispatchPolicy::Repository.sweep_inactive_partitions!(cutoff_seconds: 60, policy_name: POLICY)

    assert_equal %w[acct:10 acct:1:eu],
                 DispatchPolicy::Partition.for_policy(POLICY).order(:partition_key).pluck(:partition_key),
                 "only the drained partition was collectable; the other two still hold work"
  end

  # One bind per held partition and one transaction over all of them:
  # Postgres caps parameters at 65,535, and holding FOR UPDATE on every
  # row for the whole loop was measured at ~0.5s on 2,500 partitions with
  # a concurrent perform_later blocked behind it. Sliced, byte order is
  # preserved across slices so the stage_many! lock-order invariant still
  # holds.
  def test_the_quarantine_release_is_sliced
    batch = DispatchPolicy::Repository::QUARANTINE_RELEASE_BATCH
    keys  = (1..(batch + 5)).map { |i| format("k%05d", i) }
    keys.each do |key|
      DispatchPolicy::Repository.stage!(
        policy_name: POLICY, partition_key: key, queue_name: nil,
        job_class: "X", job_data: {}, context: {}
      )
      id = DispatchPolicy::StagedJob.where(partition_key: key).pick(:id)
      DispatchPolicy::Repository.quarantine_staged_jobs!(
        policy_name: POLICY, partition_key: key, ids: [id], reason: "t"
      )
    end
    ActiveRecord::Base.connection.execute(
      "UPDATE dispatch_policy_staged_jobs SET failed_at = now() - interval '2 hours'"
    )

    seen = capture_sql do
      DispatchPolicy::Repository.release_aged_quarantines!(policy_name: POLICY, older_than: 60)
    end

    locks = seen.select { |p| p[:name] == "lock_partitions_for_quarantine_release" }
    assert_equal 2, locks.size,
                 "one statement for #{keys.size} keys hits the bind ceiling and holds " \
                 "every lock for the whole loop"

    # The slice COUNT alone does not pin the fix: hoisting the transaction
    # around the loop still emits two lock statements while putting the
    # whole sweep back under one FOR UPDATE hold. The shape does.
    shape = seen.filter_map do |p|
      next p[:sql].strip[0, 6] if p[:name] == "TRANSACTION"

      "LOCK" if p[:name] == "lock_partitions_for_quarantine_release"
    end
    assert_equal %w[BEGIN LOCK COMMIT BEGIN LOCK COMMIT], shape,
                 "one transaction per slice, so a slow release never holds every " \
                 "partition's row lock for the whole pass"
    assert_equal keys.size, DispatchPolicy::StagedJob.deliverable.count
  end
end
