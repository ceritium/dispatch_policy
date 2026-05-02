# frozen_string_literal: true

require_relative "../test_helper"

class OperatorHintsTest < Minitest::Test
  Hints = DispatchPolicy::OperatorHints

  def call(metrics)
    Hints.for({ tick_max_duration_ms: 25_000 }.merge(metrics))
  end

  def test_no_hints_for_healthy_baseline
    hints = call(
      avg_tick_ms:         50,
      max_tick_ms:         200,
      pending_total:       0,
      admitted_per_minute: 600,
      forward_failures:    0,
      jobs_admitted:       600,
      active_partitions:   10,
      never_checked:       0,
      in_backoff:          0,
      total_partitions:    10,
      adapter_target_jps:  3500,
      pending_trend:       :flat
    )
    assert_empty hints
  end

  def test_warns_when_avg_tick_above_60_percent_of_deadline
    hints = call(avg_tick_ms: 16_000) # 64% of 25_000
    msgs = hints.map(&:message)
    assert(msgs.any? { |m| m.include?("Avg tick is") })
    assert_equal :warn, hints.find { |h| h.message.include?("Avg tick") }.level
  end

  def test_critical_when_avg_tick_above_85_percent_of_deadline
    hints = call(avg_tick_ms: 22_000) # 88% of 25_000
    h = hints.find { |x| x.message.include?("Avg tick") }
    assert_equal :critical, h.level
  end

  def test_drain_time_hint_kicks_in_when_backlog_exceeds_30_min
    hints = call(
      pending_total:       60_000,
      admitted_per_minute: 1_000 # → 60 min drain
    )
    assert(hints.any? { |h| h.message.include?("would take") })
  end

  def test_pending_growth_hint
    hints = call(
      admitted_per_minute: 100,
      pending_total:       500,
      pending_trend:       :up
    )
    assert(hints.any? { |h| h.message.include?("Inflow > outflow") })
  end

  def test_pending_growth_hint_silenced_when_backlog_drained
    # A transient spike in the tail third of the sparkline still
    # makes trend_direction return :up, even after the backlog
    # fully drained. With pending = 0, the situation is resolved.
    hints = call(
      admitted_per_minute: 100,
      pending_total:       0,
      pending_trend:       :up
    )
    refute(hints.any? { |h| h.message.include?("Inflow > outflow") })
  end

  def test_forward_failure_rate_thresholds
    warn_hints = call(jobs_admitted: 1_000, forward_failures: 20) # 2%
    crit_hints = call(jobs_admitted: 1_000, forward_failures: 100) # 10%

    warn = warn_hints.find { |h| h.message.start_with?("Forward failures") }
    crit = crit_hints.find { |h| h.message.start_with?("Forward failures") }
    assert_equal :warn,     warn.level
    assert_equal :critical, crit.level
  end

  def test_no_forward_failure_hint_below_1_percent
    hints = call(jobs_admitted: 1_000, forward_failures: 5) # 0.5%
    refute(hints.any? { |h| h.message.start_with?("Forward failures") })
  end

  def test_never_checked_partitions_warns
    hints = call(never_checked: 7, active_partitions: 200, total_partitions: 200)
    assert(hints.any? { |h| h.message.include?("never been checked") })
  end

  def test_partition_cardinality_hint_at_50k
    hints = call(total_partitions: 60_000)
    assert(hints.any? { |h| h.message.include?("partition_inactive_after") })
  end

  def test_adapter_ceiling_hint
    hints = call(
      admitted_per_minute: 180_000, # 3000/sec
      adapter_target_jps:  3500     # 86% of ceiling
    )
    assert(hints.any? { |h| h.message.include?("of the configured adapter ceiling") })
  end

  def test_no_adapter_hint_when_target_unset
    hints = call(admitted_per_minute: 200_000, adapter_target_jps: nil)
    refute(hints.any? { |h| h.message.include?("adapter ceiling") })
  end
end
