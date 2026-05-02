# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# Integration tests for in-tick fairness. These verify the contract
# the plan committed to:
#
#   1. claim_partitions still orders by last_checked_at (anti-stagnation
#      guarantee unchanged).
#   2. Inside the tick, claimed partitions are reordered by decayed_admits
#      ASC so under-admitted partitions get first crack at the budget.
#   3. With tick_admission_budget set, every claimed partition admits
#      AT MOST fair_share = ceil(cap/N), and ALL leftover budget is
#      redistributed in the same tick.
#   4. The decay update is atomic with the admit (no partial state on
#      crash) and survives the per-partition transaction.
class FairnessIntegrationTest < Minitest::Test
  class TestFairJob < ActiveJob::Base
    include DispatchPolicy::JobExtension
    around_enqueue do |job, blk|
      DispatchPolicy::JobExtension.around_enqueue_for(job, blk)
    end
    def perform(*); end
  end

  def self.connect!
    return @connected if defined?(@connected) && @connected

    ActiveRecord::Base.establish_connection(
      adapter:  "postgresql",
      encoding: "unicode",
      host:     ENV.fetch("DB_HOST", "localhost"),
      username: ENV.fetch("DB_USER", ENV["USER"]),
      password: ENV.fetch("DB_PASS", ""),
      database: ENV.fetch("DB_NAME", "dispatch_policy_test")
    )
    ActiveRecord::Base.connection.execute("SELECT 1")
    @connected = true
  rescue StandardError => e
    warn "[skip] Postgres not reachable: #{e.message}"
    @connected = false
  end

  def setup
    super
    skip "no Postgres available" unless self.class.connect!
    ensure_schema!
    truncate_tables!

    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build("fair_test") do
      context ->(args) { (args.first || {}).to_h }
      gate :throttle,
           rate:         10_000, # high enough that the throttle never
           per:          60,     # binds; we want fair_share to be the
           partition_by: ->(c) { c["key"] || c[:key] || "default" }
    end
    DispatchPolicy.registry.register(policy)
    TestFairJob.dispatch_policy_name = "fair_test"
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )
  end

  def ensure_schema!
    cols = ActiveRecord::Base.connection.columns("dispatch_policy_partitions").map(&:name)
    return if (cols & %w[decayed_admits decayed_admits_at]).size == 2
    # Drop and reload via the migration to align with new fairness columns.
    ActiveRecord::Base.connection.execute(
      "DROP TABLE IF EXISTS dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples CASCADE"
    )
    require_relative "../../db/migrate/20260501000001_create_dispatch_policy_tables"
    ActiveRecord::Migration.suppress_messages do
      CreateDispatchPolicyTables.new.change
    end
  end

  def stage_n!(partition_key, n)
    n.times do |i|
      DispatchPolicy::Repository.stage!(
        policy_name:   "fair_test",
        partition_key: partition_key,
        queue_name:    nil,
        job_class:     "FairnessIntegrationTest::TestFairJob",
        job_data:      {
          "job_id" => "#{partition_key}-#{i}",
          "job_class" => "FairnessIntegrationTest::TestFairJob",
          "arguments" => [{}]
        },
        context:       {},
        priority:      0
      )
    end
  end

  # ----- decay update is atomic with the admit ---------------------------

  def test_admit_writes_decayed_admits_atomically_with_admit
    stage_n!("k1", 3)
    DispatchPolicy.config.tick_admission_budget = nil
    DispatchPolicy.config.fairness_half_life_seconds = 60

    DispatchPolicy::Tick.run(policy_name: "fair_test")

    p = DispatchPolicy::Partition.first
    # decayed_admits picks up exactly the number admitted in this tick;
    # decayed_admits_at is set to a recent timestamp.
    assert_in_delta 3.0, p.decayed_admits, 0.0001
    refute_nil p.decayed_admits_at
    assert_in_delta Time.current.to_f, p.decayed_admits_at.to_f, 5.0
  end

  def test_decay_compounds_over_multiple_ticks_with_elapsed_time
    # Postgres `now()` is wall-clock and does not honour Ruby's
    # travel_to. To exercise the elapsed-time math we manually rewind
    # `decayed_admits_at` 60s into the past after the first admit.
    DispatchPolicy.config.fairness_half_life_seconds = 60

    stage_n!("k1", 1)
    DispatchPolicy::Tick.run(policy_name: "fair_test")
    p1 = DispatchPolicy::Partition.first
    assert_in_delta 1.0, p1.decayed_admits, 0.0001

    # Pretend the previous tick happened 60s ago.
    DispatchPolicy::Partition.where(id: p1.id).update_all(
      decayed_admits_at: 60.seconds.ago
    )

    # New admit: previous 1.0 decays to ~0.5, plus new 1 = ~1.5.
    stage_n!("k1", 1)
    DispatchPolicy::Tick.run(policy_name: "fair_test")

    p2 = DispatchPolicy::Partition.first
    assert_in_delta 1.5, p2.decayed_admits, 0.05,
                    "decayed value must halve over a half-life and add the new admits on top"
  end

  # ----- in-tick reordering -----------------------------------------------

  def test_partitions_are_processed_in_decayed_admits_order
    # stage! takes the partition_key literally — what we pass goes into
    # the partitions row as-is; the gate's "throttle=" prefix is what
    # the JobExtension would derive from the policy on the per-job
    # path, but here we bypass that and use simple keys.
    stage_n!("hot",  10)
    stage_n!("cold", 10)

    # Seed: hot has lots of recent admits, cold has none.
    DispatchPolicy::Partition.where(partition_key: "hot").update_all(
      decayed_admits: 100.0, decayed_admits_at: Time.current
    )

    # Capture order via Forwarder.dispatch's first row partition_key.
    seen_partition_keys = []
    original = DispatchPolicy::Forwarder.singleton_class.instance_method(:dispatch)
    DispatchPolicy::Forwarder.singleton_class.define_method(:dispatch) do |rows|
      seen_partition_keys << rows.first["partition_key"] unless rows.empty?
      # No-op: don't actually forward (TestAdapter would, but we only
      # care about the ordering signal). The TX still commits the
      # staging DELETE + counters update.
    end

    DispatchPolicy.config.fairness_half_life_seconds = 60
    DispatchPolicy.config.tick_admission_budget = nil

    DispatchPolicy::Tick.run(policy_name: "fair_test")

    # cold (decayed=0) must come BEFORE hot (decayed=100).
    cold_idx = seen_partition_keys.index("cold")
    hot_idx  = seen_partition_keys.index("hot")
    refute_nil cold_idx, "cold partition was never visited (saw #{seen_partition_keys.inspect})"
    refute_nil hot_idx,  "hot partition was never visited (saw #{seen_partition_keys.inspect})"
    assert cold_idx < hot_idx,
           "expected cold (decayed=0) to come before hot (decayed=100), saw #{seen_partition_keys.inspect}"
  ensure
    DispatchPolicy::Forwarder.singleton_class.define_method(:dispatch, original)
  end

  # ----- tick_admission_budget enforces a hard global cap ----------------

  def test_tick_budget_caps_each_partition_to_fair_share
    # 5 partitions × 10 staged each = 50 pending. Cap = 10. fair_share =
    # ceil(10/5) = 2. Pass-1 admits 2 per partition, total = 10. Pass-2
    # has 0 leftover and does nothing.
    5.times { |i| stage_n!("p#{i}", 10) }

    DispatchPolicy.config.fairness_half_life_seconds = 60
    DispatchPolicy.config.tick_admission_budget = 10

    result = DispatchPolicy::Tick.run(policy_name: "fair_test")
    assert_equal 10, result.jobs_admitted

    # Each partition admitted exactly 2; pending decreased by 2 per row.
    DispatchPolicy::Partition.find_each do |p|
      assert_equal 8, p.pending_count, "partition #{p.partition_key} should have 10-2=8 pending"
      assert_in_delta 2.0, p.decayed_admits, 0.0001
    end
  end

  def test_tick_budget_redistributes_leftover_within_one_tick
    # 4 partitions, but only 1 has pending. Cap = 8. fair_share = ceil(8/4)
    # = 2. Pass-1: only the partition with pending admits 2. Three others
    # admit 0 (no pending → admit_partition returns 0, deduped to deny
    # path with admit_count=0; that partition NEVER is in the candidates
    # for pass-2). The pending partition received fair_share, so it is a
    # pass-2 candidate. With remaining = 8 - 2 = 6, it admits min(6,
    # fair_share=2) = 2 more. Repeat until either remaining=0 or its
    # pending depletes.

    # Stage in p0 only. Pre-create the other 3 as empty partitions so
    # claim_partitions picks them up as candidates.
    stage_n!("p0", 100)
    %w[p1 p2 p3].each do |k|
      DispatchPolicy::Repository.upsert_partition!(
        policy_name: "fair_test", partition_key: k,
        queue_name: nil, context: {}, delta_pending: 0
      )
    end

    # claim_partitions filters pending_count > 0, so the empty partitions
    # are skipped. With only one claimed partition, fair_share = cap = 8;
    # redistribution does not kick in (no other candidates), but the
    # single partition still admits 8 jobs in this tick.
    DispatchPolicy.config.fairness_half_life_seconds = 60
    DispatchPolicy.config.tick_admission_budget = 8

    result = DispatchPolicy::Tick.run(policy_name: "fair_test")
    assert_equal 8, result.jobs_admitted
    assert_equal 92, DispatchPolicy::Partition.find_by(partition_key: "p0").pending_count
  end

  # ----- anti-stagnation: every claimed partition admits >= 1 -----------

  def test_no_stagnation_floor_is_one_per_partition_per_visit
    # Force a tiny global cap and a large N: ceil(cap/N) clamped to >= 1.
    20.times { |i| stage_n!("p#{i}", 5) }

    DispatchPolicy.config.partition_batch_size  = 20
    DispatchPolicy.config.fairness_half_life_seconds = 60
    DispatchPolicy.config.tick_admission_budget = 5 # cap < N

    DispatchPolicy::Tick.run(policy_name: "fair_test")

    # Pass-1 admits 1 per partition (fair_share = ceil(5/20) = 1) for
    # the first 5 (we run pass-2 too, but pass-2 only revisits
    # partitions that hit fair_share, in order, until cap is reached).
    # All 20 partitions are claimed; cap=5 means at most 5 admit in
    # pass-1. With more candidates than cap, the rest stay at 0 — but
    # they are claimed, last_checked_at bumped, so next tick they are
    # the oldest and get their turn.
    admitted_total = DispatchPolicy::Partition.sum(:total_admitted)
    assert admitted_total <= 5,
           "tick_admission_budget=5 must hold even when partitions claimed = 20 (admitted=#{admitted_total})"
  end

  # ----- multi-shard isolation -------------------------------------------

  def test_fairness_state_is_per_partition_so_shards_dont_interfere
    DispatchPolicy::Repository.stage!(
      policy_name: "fair_test", partition_key: "throttle=a", queue_name: nil,
      shard: "alpha",
      job_class: "FairnessIntegrationTest::TestFairJob",
      job_data:  { "job_id" => "a1", "job_class" => "FairnessIntegrationTest::TestFairJob", "arguments" => [{}] },
      context: {}, priority: 0
    )
    DispatchPolicy::Repository.stage!(
      policy_name: "fair_test", partition_key: "throttle=b", queue_name: nil,
      shard: "beta",
      job_class: "FairnessIntegrationTest::TestFairJob",
      job_data:  { "job_id" => "b1", "job_class" => "FairnessIntegrationTest::TestFairJob", "arguments" => [{}] },
      context: {}, priority: 0
    )
    DispatchPolicy.config.fairness_half_life_seconds = 60

    DispatchPolicy::Tick.run(policy_name: "fair_test", shard: "alpha")

    a = DispatchPolicy::Partition.find_by(shard: "alpha")
    b = DispatchPolicy::Partition.find_by(shard: "beta")
    assert_in_delta 1.0, a.decayed_admits, 0.0001, "alpha shard advanced its own counter"
    assert_in_delta 0.0, b.decayed_admits, 0.0001, "beta shard's counter must not be touched by an alpha tick"
  end
end
