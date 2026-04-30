# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class TickRunTest < ActiveSupport::TestCase
    test "TickLoop flushes buffered samples on graceful exit" do
      iterations = 0
      assert_difference -> { TickRun.count }, +1 do
        TickLoop.run(
          sleep_for:      0,
          sleep_for_busy: 0,
          stop_when: -> {
            iterations += 1
            iterations >= 2  # one body iteration emits one tick.dispatch_policy
          }
        )
      end

      row = TickRun.order(:started_at).last
      assert_equal 0, row.admitted
      assert_kind_of Float, row.duration_ms
      assert row.duration_ms >= 0
      refute row.declined
      assert_nil row.error_class
    end

    test "Stats.tick_runs aggregates samples into p50/p95/p99" do
      now = Time.current
      TickRun.insert_all([
        { started_at: now, duration_ms:  5.0, admitted: 1, partitions: 1,
          declined: false, error_class: nil, error_message: nil,
          policy_name: nil, created_at: now },
        { started_at: now, duration_ms: 10.0, admitted: 2, partitions: 1,
          declined: false, error_class: nil, error_message: nil,
          policy_name: nil, created_at: now },
        { started_at: now, duration_ms: 20.0, admitted: 0, partitions: 0,
          declined: true,  error_class: nil, error_message: nil,
          policy_name: nil, created_at: now },
        { started_at: now, duration_ms: 50.0, admitted: 5, partitions: 3,
          declined: false, error_class: "RuntimeError",
          error_message: "boom", policy_name: nil, created_at: now }
      ])

      result = Stats.tick_runs(window: 60)

      assert_equal 4, result[:ticks]
      assert_equal 8, result[:admitted]
      assert_equal 5, result[:partitions]
      assert_equal 1, result[:declined]
      assert_equal 1, result[:errored]
      # Linear-interp p50 of [5, 10, 20, 50] is 15.0 (idx 1.5 between 10 and 20).
      assert_in_delta 15.0, result[:p50_ms], 0.01
      assert_equal 50.0, result[:max_ms]
    end

    test "Stats.tick_runs returns zeros when there are no samples" do
      result = Stats.tick_runs(window: 60)
      assert_equal 0, result[:ticks]
      assert_nil result[:p50_ms]
      assert_equal({}, result[:per_policy])
    end

    test "Stats.tick_runs filters by policy_name" do
      now = Time.current
      TickRun.insert_all([
        { started_at: now, duration_ms: 5.0,  admitted: 1, partitions: 1,
          declined: false, error_class: nil, error_message: nil,
          policy_name: "a", created_at: now },
        { started_at: now, duration_ms: 10.0, admitted: 2, partitions: 1,
          declined: false, error_class: nil, error_message: nil,
          policy_name: "b", created_at: now }
      ])

      a = Stats.tick_runs(window: 60, policy_name: "a")
      assert_equal 1, a[:ticks]
      assert_equal 1, a[:admitted]
    end
  end
end
