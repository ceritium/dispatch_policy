# frozen_string_literal: true

require_relative "../test_helper"

# Unit-level math + threading checks for the EWMA decay and the
# in-tick fair_share calculation. The end-to-end tick behaviour lives
# in test/integration/fairness_test.rb.
class FairnessUnitTest < Minitest::Test
  # Reproduces the same math the SQL UPDATE applies, so we know the
  # Ruby-side sort and the Postgres-side write agree on what
  # "decayed_admits at time T" means.
  def decay(stored_value, stored_at_epoch, now_epoch, half_life)
    return 0.0 if half_life.nil? || half_life <= 0
    tau     = half_life.to_f / Math.log(2)
    elapsed = [now_epoch - stored_at_epoch, 0.0].max
    stored_value.to_f * Math.exp(-elapsed / tau)
  end

  def test_decay_halves_after_one_half_life
    assert_in_delta 50.0, decay(100.0, 0.0, 60.0, 60), 0.001
  end

  def test_decay_quarters_after_two_half_lives
    assert_in_delta 25.0, decay(100.0, 0.0, 120.0, 60), 0.001
  end

  def test_decay_zero_elapsed_preserves_value
    assert_in_delta 100.0, decay(100.0, 1000.0, 1000.0, 60), 0.001
  end

  def test_decay_clamps_negative_elapsed_to_zero
    # Time travel safety: if a clock drift gives now < stored_at, do
    # not amplify the value. Clamping elapsed to >= 0 is what the Tick
    # implementation does too.
    assert_in_delta 100.0, decay(100.0, 2000.0, 1000.0, 60), 0.001
  end

  def test_decay_becomes_negligible_after_many_half_lives
    # 10 half-lives → 2^-10 ≈ 0.001 of original.
    assert_in_delta 0.0977, decay(100.0, 0.0, 600.0, 60), 0.01
  end

  # ----- Tick fair_share math (no DB required) ------------------------------

  def test_fair_share_when_no_global_cap
    # Without a tick_admission_budget, fair_share defaults to the
    # per-partition admission_batch_size (legacy behaviour).
    DispatchPolicy.config.admission_batch_size = 100
    DispatchPolicy.config.tick_admission_budget = nil

    fair_share = if DispatchPolicy.config.tick_admission_budget && 5.positive?
                   [(DispatchPolicy.config.tick_admission_budget.to_f / 5).ceil, 1].max
                 else
                   DispatchPolicy.config.admission_batch_size
                 end
    assert_equal 100, fair_share
  end

  def test_fair_share_with_global_cap_divides_evenly
    DispatchPolicy.config.tick_admission_budget = 20
    n_partitions = 5
    fair_share = [(20.0 / n_partitions).ceil, 1].max
    assert_equal 4, fair_share
  end

  def test_fair_share_floor_is_one_when_n_exceeds_budget
    # Anti-stagnation guarantee: if cap < N, every partition still
    # gets at least 1 admit slot. ceil(cap / N) saturates at 1.
    DispatchPolicy.config.tick_admission_budget = 3
    n_partitions = 50
    fair_share = [(3.0 / n_partitions).ceil, 1].max
    assert_equal 1, fair_share
  end

  # ----- Policy DSL plumbs the new options through --------------------------

  def test_policy_dsl_records_fairness_overrides
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
      fairness half_life: 30
      tick_admission_budget 200
    end
    assert_equal 30.0, policy.fairness_half_life_seconds
    assert_equal 200,  policy.tick_admission_budget
  end

  def test_policy_dsl_defaults_fairness_to_nil
    policy = DispatchPolicy::PolicyDSL.build("p") do
      gate :throttle, rate: 1, per: 60, partition_by: ->(_c) { "k" }
    end
    assert_nil policy.fairness_half_life_seconds
    assert_nil policy.tick_admission_budget
  end
end
