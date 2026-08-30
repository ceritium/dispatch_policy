# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"

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
    %w[zeta alpha mike].each do |key|
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
        %w[zeta alpha mike].map do |key|
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

    keys = seen[lock][:binds].map { |b| b.respond_to?(:value) ? b.value : b }
                             .reject { |v| v == POLICY }
    assert_equal %w[alpha mike zeta], keys,
                 "the order has to match what stage_many! sorts by, or the two still cross"
  end

  # The ordering must not have changed what the flush actually records.
  def test_the_deny_still_records_backoff_and_patch_for_every_entry
    DispatchPolicy::Repository.bulk_record_partition_denies!([
      { policy_name: POLICY, partition_key: "alpha",
        gate_state_patch: { "throttle" => { "tokens" => 3.0 } }, retry_after: 30 },
      { policy_name: POLICY, partition_key: "zeta",
        gate_state_patch: {}, retry_after: 60 }
    ])

    alpha = DispatchPolicy::Partition.find_by(partition_key: "alpha")
    zeta  = DispatchPolicy::Partition.find_by(partition_key: "zeta")

    assert_in_delta 3.0, alpha.gate_state.dig("throttle", "tokens").to_f, 0.01
    refute_nil alpha.next_eligible_at
    refute_nil zeta.next_eligible_at
    assert_operator zeta.next_eligible_at, :>, alpha.next_eligible_at
  end
end
