# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"
require_relative "../../app/models/dispatch_policy/adaptive_concurrency_stats"

# Proves the two mechanisms compose without state corruption:
#
#   - in-tick fairness writes `partitions.decayed_admits` per tick
#   - :adaptive_concurrency writes `dispatch_policy_adaptive_concurrency_stats`
#     per perform via record_observation
#
# Tables, locks, code paths are all distinct — the only contract is
# that the per-partition admit_count comes out as
# `min(fair_share, current_max - in_flight)`.
class AdaptiveWithFairnessIntegrationTest < Minitest::Test
  POLICY = "adaptive_fair_test"

  # Concrete job class so Forwarder.dispatch can deserialize the staged
  # rows. Marked at top level with a stable name; the policy looks
  # this up via the registered name, not via this constant.
  class BenchJob < ActiveJob::Base
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
    truncate_tables!

    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build(POLICY) do
      context ->(args) { (args.first || {}).to_h }
      partition_by ->(c) { c["tenant"] || c[:tenant] || "default" }
      gate :adaptive_concurrency, initial_max: 5, target_lag_ms: 1000, min: 1
      fairness half_life: 60
      tick_admission_budget 12
    end
    DispatchPolicy.registry.register(policy)
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples, " \
      "dispatch_policy_adaptive_concurrency_stats RESTART IDENTITY"
    )
  end

  def stage_n!(tenant, n)
    n.times do |i|
      DispatchPolicy::Repository.stage!(
        policy_name:   POLICY,
        partition_key: tenant,
        queue_name:    nil,
        job_class:     "AdaptiveWithFairnessIntegrationTest::BenchJob",
        job_data:      { "job_id" => "#{tenant}-#{i}", "job_class" => "AdaptiveWithFairnessIntegrationTest::BenchJob", "arguments" => [{}] },
        context:       { "tenant" => tenant },
        priority:      0
      )
    end
  end

  # ----- both updates are atomic with the admit -----------------------------

  def test_admit_writes_to_both_stats_tables
    # 4 tenants, 10 staged each. fair_share = ceil(12/4) = 3, but
    # adaptive's initial_max=5 so adaptive lets 5 through. min(3, 5) = 3.
    %w[A B C D].each { |t| stage_n!(t, 10) }

    DispatchPolicy::Tick.run(policy_name: POLICY)

    # Each partition has decayed_admits set (fairness write):
    DispatchPolicy::Partition.where(policy_name: POLICY).find_each do |p|
      assert_equal 3.0, p.decayed_admits, "fair_share=3 caps each partition's admit"
      refute_nil p.decayed_admits_at
    end

    # Each partition has been seeded in adaptive_stats (adaptive write,
    # at evaluate time — record_observation hasn't fired yet because
    # nothing has performed):
    seeds = DispatchPolicy::AdaptiveConcurrencyStats.where(policy_name: POLICY).pluck(:partition_key, :current_max)
    assert_equal %w[A B C D].sort, seeds.map(&:first).sort
    assert seeds.all? { |_, c| c == 5 }, "all stats rows seeded at initial_max=5"
  end

  # ----- adaptive cap binds when current_max < fair_share ------------------

  def test_adaptive_cap_wins_when_lower_than_fair_share
    # Pre-shrink A's current_max to 2 AND give it an inflight row so the
    # gate's safety valve doesn't kick in (it only fires when in_flight=0).
    # fair_share=3, current_max-in_flight = 2-0 = 2 → A admits 2.
    %w[A B C D].each { |t| stage_n!(t, 10) }
    DispatchPolicy::Repository.adaptive_seed!(
      policy_name: POLICY, partition_key: "A", initial_max: 5
    )
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_adaptive_concurrency_stats
      SET current_max = 2 WHERE policy_name = '#{POLICY}' AND partition_key = 'A'
    SQL
    # Pre-insert an inflight row so in_flight=0 doesn't trigger the
    # safety valve (which would floor remaining at initial_max=5).
    DispatchPolicy::Repository.insert_inflight!([
      { policy_name: POLICY, partition_key: "A", active_job_id: SecureRandom.uuid }
    ])

    DispatchPolicy::Tick.run(policy_name: POLICY)

    a = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: "A")
    others = DispatchPolicy::Partition.where(policy_name: POLICY).where.not(partition_key: "A")

    # current_max=2, in_flight=1 → remaining=1. min(fair_share=3, 1) = 1.
    assert_in_delta 1.0, a.decayed_admits, 0.001,
                    "adaptive caps A at 1 (current_max=2 - in_flight=1), not 3 (fair_share)"

    # Each non-A partition admits at least fair_share=3. With tick_cap=12
    # and A only used 1, pass-2 redistributes the leftover 2 to whoever
    # filled their share — bumping at least one of them above fair_share.
    others.each do |p|
      assert p.decayed_admits >= 3.0,
             "each non-A partition admits at least fair_share=3 (got #{p.decayed_admits})"
    end

    # Total never exceeds the tick_admission_budget.
    total = DispatchPolicy::Partition.where(policy_name: POLICY).sum(:decayed_admits)
    assert total <= 12.001, "total admits ≤ tick_admission_budget=12 (got #{total})"
  end

  # ----- fair_share binds when current_max > fair_share --------------------

  def test_fair_share_wins_when_lower_than_adaptive_cap
    # Larger initial_max so adaptive doesn't bind.
    DispatchPolicy.reset_registry!
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build(POLICY) do
        partition_by ->(c) { c["tenant"] || c[:tenant] || "default" }
        gate :adaptive_concurrency, initial_max: 100, target_lag_ms: 1000, min: 1
        fairness half_life: 60
        tick_admission_budget 12
      end
    )
    %w[A B C D].each { |t| stage_n!(t, 50) }

    DispatchPolicy::Tick.run(policy_name: POLICY)

    DispatchPolicy::Partition.where(policy_name: POLICY).find_each do |p|
      assert_equal 3.0, p.decayed_admits,
                   "fair_share=ceil(12/4)=3 binds — adaptive (initial_max=100) does not"
    end
  end

  # ----- safety valve coexists with fair_share ----------------------------

  def test_safety_valve_when_inflight_zero_with_shrunken_cap
    # current_max shrunk to 1, in_flight=0 → gate's safety valve floors
    # remaining at initial_max=5. fair_share=3 caps it from above.
    # Net: admits 3 (fair_share wins).
    stage_n!("A", 10)
    DispatchPolicy::Repository.adaptive_seed!(
      policy_name: POLICY, partition_key: "A", initial_max: 5
    )
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE dispatch_policy_adaptive_concurrency_stats
      SET current_max = 1 WHERE policy_name = '#{POLICY}' AND partition_key = 'A'
    SQL

    DispatchPolicy::Tick.run(policy_name: POLICY)

    a = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: "A")
    # With only 1 partition claimed, fair_share = ceil(12/1) = 12. The
    # adaptive safety valve grants up to initial_max=5 (vs the shrunken
    # current_max=1). min(12, 5) = 5.
    assert_equal 5.0, a.decayed_admits,
                 "safety valve floors remaining at initial_max=5 even when current_max shrunk to 1"
  end

  # ----- decayed_admits reflects actual admits, not fair_share -----------

  def test_decayed_admits_records_actual_admit_count_not_fair_share
    # Tenant A has only 2 staged jobs but fair_share=3. Admits 2.
    # decayed_admits should be 2, not 3.
    stage_n!("A", 2)
    %w[B C D].each { |t| stage_n!(t, 10) }

    DispatchPolicy::Tick.run(policy_name: POLICY)

    a = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: "A")
    assert_equal 2.0, a.decayed_admits,
                 "EWMA tracks reality (2 admitted), not fair_share (3)"
  end
end
