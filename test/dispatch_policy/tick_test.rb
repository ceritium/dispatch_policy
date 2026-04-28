# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class TickTest < ActiveSupport::TestCase
    class FairJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }
      end
      def perform(*); end
    end

    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    setup do
      @orig_quantum    = DispatchPolicy.config.round_robin_quantum
      @orig_batch_size = DispatchPolicy.config.batch_size
    end

    teardown do
      DispatchPolicy.config.round_robin_quantum = @orig_quantum
      DispatchPolicy.config.batch_size          = @orig_batch_size
    end

    test "each key gets at least quantum when batch_size is tight" do
      DispatchPolicy.config.round_robin_quantum = 3
      DispatchPolicy.config.batch_size          = 9

      20.times { FairJob.perform_later("A") }
      20.times { FairJob.perform_later("B") }
      20.times { FairJob.perform_later("C") }

      Tick.run(policy_name: FairJob.resolved_dispatch_policy.name)
      grouped = StagedJob.admitted.group(:round_robin_key).count

      assert_equal 3, grouped["A"]
      assert_equal 3, grouped["B"]
      assert_equal 3, grouped["C"]
    end

    test "top-up fills unused batch slots beyond quantum for a single tenant" do
      DispatchPolicy.config.round_robin_quantum = 5
      DispatchPolicy.config.batch_size          = 40

      60.times { FairJob.perform_later("only") }
      Tick.run(policy_name: FairJob.resolved_dispatch_policy.name)
      assert_equal 40, StagedJob.admitted.count
    end

    class TimeWeightedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }, weight: :time
      end
      def perform(*); end
    end

    test "weight: :time gives bigger quanta to partitions with less consumed time" do
      DispatchPolicy.config.batch_size = 40

      # Heavy partition has 20s of consumed time recently; light has none.
      # Inverse-weighted, light should claim ~99% of the batch_size budget.
      PartitionObservation.observe!(
        policy_name: TimeWeightedJob.resolved_dispatch_policy.name,
        partition_key: "heavy", queue_lag_ms: 0, duration_ms: 20_000
      )

      30.times { TimeWeightedJob.perform_later("heavy") }
      30.times { TimeWeightedJob.perform_later("light") }

      Tick.run(policy_name: TimeWeightedJob.resolved_dispatch_policy.name)

      grouped = StagedJob.admitted.group(:round_robin_key).count
      assert_operator grouped["light"].to_i, :>, grouped["heavy"].to_i,
        "light should be admitted more than heavy when heavy has burned 20s (got #{grouped.inspect})"
    end

    test "weight: :time with a solo tenant fetches up to batch_size" do
      DispatchPolicy.config.batch_size = 25

      40.times { TimeWeightedJob.perform_later("solo") }
      Tick.run(policy_name: TimeWeightedJob.resolved_dispatch_policy.name)

      assert_equal 25, StagedJob.admitted.count
    end

    test "time-weighted fetch issues a constant number of SELECTs regardless of partition count" do
      # Architectural assertion: fetch_time_weighted_batch must be O(1)
      # in SELECT round-trips, not O(partitions). A regression that
      # reintroduces a per-partition loop will fire one SELECT per
      # partition and trip this test.
      DispatchPolicy.config.batch_size = 50

      partition_count = 40
      partition_count.times do |i|
        2.times { TimeWeightedJob.perform_later("p#{i}") }
      end

      staged_selects = []
      callback = ->(_, _, _, _, payload) {
        sql = payload[:sql]
        next if payload[:name] == "SCHEMA"
        next unless sql.include?("dispatch_policy_staged_jobs")
        # Match SELECTs that filter on admitted_at IS NULL — those are
        # the batch-fetch queries we care about.
        staged_selects << sql if /^\s*(WITH|SELECT)/i.match?(sql) && sql.include?("admitted_at IS NULL")
      }

      ActiveSupport::Notifications.subscribed(callback, "sql.active_record") do
        Tick.run(policy_name: TimeWeightedJob.resolved_dispatch_policy.name)
      end

      # Expected SELECTs per Tick.run for time-weighted with N partitions:
      #   1 — distinct round_robin_key pluck (active partitions)
      #   1 — CTE+LATERAL batch fetch
      #   0 or 1 — top-up (only if quanta sum < batch_size)
      # Cap at 5 to leave headroom for incidental queries (e.g. consumed_ms
      # lookup in PartitionObservation if it ever scans staged_jobs).
      assert_operator staged_selects.size, :<=, 5,
        "fetch must be O(1) SELECTs, not O(partitions=#{partition_count}). " \
        "Got #{staged_selects.size}:\n#{staged_selects.join("\n")}"
    end

    test "concurrency gate caps admissions per partition" do
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")
      CappedJob.perform_later("X")

      Tick.run(policy_name: CappedJob.resolved_dispatch_policy.name)
      assert_equal 1, StagedJob.admitted.count
      assert_equal 2, StagedJob.pending.count
    end
  end
end
