# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"

# The shard is routing metadata, not identity — but it was pinned on
# first write and never rewritten. So the day a policy gains a
# `shard_by`, which is the documented way to parallelise it across worker
# pools, every partition that already existed keeps `default` while the
# tick loops are started for the new shard names. `claim_partitions`
# filters on shard, nothing rewrites it, and NEW partitions get the new
# shard and drain normally — so the dashboard looks healthy while every
# existing tenant goes silent, permanently.
class ShardChangeTest < DispatchPolicy::IntegrationTest
  POLICY = "shard_change"

  def stage!(shard:, key: "acct:1")
    DispatchPolicy::Repository.stage!(
      policy_name: POLICY, partition_key: key, queue_name: nil,
      job_class: "X", job_data: {}, context: {}, shard: shard
    )
  end

  def shard_of(key = "acct:1")
    DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: key).shard
  end

  def drain!(key = "acct:1")
    DispatchPolicy::Repository.claim_staged_jobs!(
      policy_name: POLICY, partition_key: key, limit: 100,
      gate_state_patch: {}, retry_after: nil
    )
  end

  def test_a_drained_partition_follows_a_new_shard_by
    stage!(shard: "default")
    assert_equal "default", shard_of

    drain!
    assert_equal 0, DispatchPolicy::Partition.find_by(partition_key: "acct:1").pending_count

    # The deploy that introduces shard_by; the next enqueue carries it.
    stage!(shard: "events-shard-2")

    assert_equal "events-shard-2", shard_of,
                 "a drained partition that never re-shards is invisible to every " \
                 "loop started for the new shard names"
  end

  # The pin is what stops a partition moving out from under a tick that is
  # mid-claim, so it has to survive while there is work.
  def test_a_partition_holding_work_keeps_its_shard
    stage!(shard: "default")
    stage!(shard: "events-shard-2")

    assert_equal "default", shard_of,
                 "re-shard a partition with pending work and a tick loop loses it mid-drain"
  end
end
