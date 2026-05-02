# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"
require_relative "../../app/models/dispatch_policy/adaptive_concurrency_stats"

# Exercises the AIMD UPDATE in real Postgres. The unit tests stub
# Repository so they stay DB-less; here we run the SQL itself and
# pin its arithmetic across all four code branches (seed, +1 grow,
# slow-shrink, fail-shrink).
class AdaptiveConcurrencyIntegrationTest < Minitest::Test
  POLICY = "adaptive_test"

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
  end

  def ensure_schema!
    cols_present = ActiveRecord::Base.connection.table_exists?("dispatch_policy_adaptive_concurrency_stats")
    return if cols_present
    ActiveRecord::Base.connection.execute(
      "DROP TABLE IF EXISTS dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples, " \
      "dispatch_policy_adaptive_concurrency_stats CASCADE"
    )
    require_relative "../../db/migrate/20260501000001_create_dispatch_policy_tables"
    ActiveRecord::Migration.suppress_messages { CreateDispatchPolicyTables.new.change }
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples, " \
      "dispatch_policy_adaptive_concurrency_stats RESTART IDENTITY"
    )
  end

  def seed!(initial_max: 4)
    DispatchPolicy::Repository.adaptive_seed!(
      policy_name:   POLICY,
      partition_key: "k",
      initial_max:   initial_max
    )
  end

  def record!(queue_lag_ms:, succeeded:, alpha: 0.5, target: 1000.0,
              fail_factor: 0.5, slow_factor: 0.95, min: 1)
    DispatchPolicy::Repository.adaptive_record!(
      policy_name:   POLICY,
      partition_key: "k",
      queue_lag_ms:  queue_lag_ms,
      succeeded:     succeeded,
      alpha:         alpha,
      target_lag_ms: target,
      fail_factor:   fail_factor,
      slow_factor:   slow_factor,
      min:           min
    )
  end

  def stats
    DispatchPolicy::AdaptiveConcurrencyStats.find_by(policy_name: POLICY, partition_key: "k")
  end

  # ----- seed semantics ---------------------------------------------------

  def test_seed_creates_row_only_once
    seed!(initial_max: 4)
    seed!(initial_max: 99) # idempotent — must NOT bump existing row

    row = stats
    assert_equal 4,   row.current_max
    assert_equal 0.0, row.ewma_latency_ms
    assert_equal 0,   row.sample_count
  end

  # ----- additive grow ----------------------------------------------------

  def test_fast_success_grows_current_max_by_one
    seed!(initial_max: 4)
    record!(queue_lag_ms: 0, succeeded: true)

    row = stats
    assert_equal 5, row.current_max,
                 "succeeded + lag below target_lag_ms must add 1"
    assert_equal 1, row.sample_count
    assert_in_delta 0.0, row.ewma_latency_ms, 0.001 # alpha=0.5 of 0 = 0
  end

  def test_repeated_fast_success_grows_unbounded
    seed!(initial_max: 4)
    10.times { record!(queue_lag_ms: 10, succeeded: true) }

    row = stats
    assert_equal 14, row.current_max,
                 "10 fast samples should grow 4 → 14 monotonically"
    assert_equal 10, row.sample_count
  end

  # ----- slow-shrink ------------------------------------------------------

  def test_overload_shrinks_current_max_multiplicatively
    seed!(initial_max: 100)
    # Single observation with lag well above target → ewma jumps to
    # alpha * lag = 0.5 * 5000 = 2500 > target=1000 → slow-shrink.
    record!(queue_lag_ms: 5000, succeeded: true, slow_factor: 0.95)

    row = stats
    assert_equal 95, row.current_max, "FLOOR(100 * 0.95) = 95"
    assert_in_delta 2500.0, row.ewma_latency_ms, 0.001
  end

  def test_repeated_overload_shrinks_geometrically
    seed!(initial_max: 100)
    20.times { record!(queue_lag_ms: 5000, succeeded: true, slow_factor: 0.95) }

    row = stats
    # 100 → 95 → 90 → 85 → 80 → 76 → 72 → 68 → 64 → 60 → 57 → 54 →
    # 51 → 48 → 45 → 42 → 39 → 37 → 35 → 33 → 31
    # (each step is FLOOR(prev * 0.95); 20 ticks land near 35-40).
    assert_operator row.current_max, :<, 50, "20 overload obs must shrink the cap meaningfully"
    assert_operator row.current_max, :>, 1,  "but never below min=1 in only 20 steps from 100"
  end

  # ----- failure path ----------------------------------------------------

  def test_failure_halves_current_max
    seed!(initial_max: 10)
    record!(queue_lag_ms: 0, succeeded: false, fail_factor: 0.5)

    row = stats
    assert_equal 5, row.current_max, "FLOOR(10 * 0.5) = 5"
  end

  def test_failure_clamps_to_min
    seed!(initial_max: 1)
    record!(queue_lag_ms: 0, succeeded: false, fail_factor: 0.5, min: 1)

    row = stats
    assert_equal 1, row.current_max,
                 "shrinking from min must clamp at min, not 0"
  end

  # ----- EWMA math --------------------------------------------------------

  def test_ewma_blends_old_and_new_samples
    seed!(initial_max: 100)
    record!(queue_lag_ms: 1000, succeeded: true) # ewma = 0.5*1000 = 500
    row1 = stats
    assert_in_delta 500.0, row1.ewma_latency_ms, 0.001

    record!(queue_lag_ms: 0, succeeded: true)    # ewma = 500*0.5 + 0.5*0 = 250
    row2 = stats
    assert_in_delta 250.0, row2.ewma_latency_ms, 0.001
  end

  # ----- decision branching uses the NEW ewma value, not the old one ------

  def test_overload_branch_compares_against_post_update_ewma
    # Seed at 10. Apply one big lag — ewma becomes 0.5 * 5000 = 2500
    # which exceeds target=1000 — so this same UPDATE must take the
    # slow-shrink branch using the post-update ewma, NOT the pre-update
    # ewma (which was 0). If the SQL accidentally checked the old
    # ewma, this row would have grown to 11 instead of shrunk.
    seed!(initial_max: 10)
    record!(queue_lag_ms: 5000, succeeded: true, target: 1000.0, slow_factor: 0.95)

    row = stats
    assert_equal 9, row.current_max,
                 "the CASE branch must use the post-update ewma to decide; FLOOR(10*0.95)=9"
  end
end
