# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  # End-to-end tests for gate combinations and the time-bucket
  # aggregations that drive the time-weighted features. Splits into
  # three groups:
  #
  # 1. PartitionObservation boundary unit tests — exercise the
  #    minute_bucket window logic directly with explicit time travel.
  #    These are the layer that would have surfaced the bug fixed in
  #    PR #11 without depending on lucky CI timing.
  #
  # 2. Gate combination integration tests — stage jobs through the
  #    policy and tick, asserting the documented composition rules
  #    actually produce the expected admission outcomes.
  #
  # 3. Time-sensitive integration tests — same combinations but with
  #    travel_to across a minute boundary, to catch regressions in
  #    how observation-driven gates aggregate at the seam.
  class GatesIntegrationTest < ActiveSupport::TestCase
    include ActiveSupport::Testing::TimeHelpers

    setup    { @orig_batch_size = DispatchPolicy.config.batch_size }
    teardown { DispatchPolicy.config.batch_size = @orig_batch_size }

    # ───── Group 1: PartitionObservation boundary tests ─────

    test "observe! places the row in the floored minute bucket" do
      travel_to Time.utc(2026, 4, 25, 21, 1, 30) do
        PartitionObservation.observe!(
          policy_name: "p", partition_key: "X",
          queue_lag_ms: 100, duration_ms: 250
        )
      end
      row = PartitionObservation.find_by!(policy_name: "p", partition_key: "X")
      assert_equal Time.utc(2026, 4, 25, 21, 1, 0), row.minute_bucket
      assert_equal 100, row.total_lag_ms
      assert_equal 250, row.total_duration_ms
      assert_equal 1, row.observation_count
    end

    test "observe! aggregates multiple writes into the same minute bucket" do
      bucket_start = Time.utc(2026, 4, 25, 21, 1, 0)
      travel_to bucket_start + 5 do
        3.times { PartitionObservation.observe!(policy_name: "p", partition_key: "X", queue_lag_ms: 100, duration_ms: 50) }
      end
      travel_to bucket_start + 45 do
        2.times { PartitionObservation.observe!(policy_name: "p", partition_key: "X", queue_lag_ms: 200, duration_ms: 100) }
      end

      assert_equal 1, PartitionObservation.where(policy_name: "p", partition_key: "X").count
      row = PartitionObservation.find_by!(policy_name: "p", partition_key: "X")
      assert_equal 5, row.observation_count
      assert_equal 700, row.total_lag_ms      # 3·100 + 2·200
      assert_equal 350, row.total_duration_ms # 3·50  + 2·100
      assert_equal 200, row.max_lag_ms
      assert_equal 100, row.max_duration_ms
    end

    test "observe! is a no-op when partition_key is nil or empty" do
      PartitionObservation.observe!(policy_name: "p", partition_key: "",  queue_lag_ms: 1, duration_ms: 1)
      PartitionObservation.observe!(policy_name: "p", partition_key: nil, queue_lag_ms: 1, duration_ms: 1)
      assert_equal 0, PartitionObservation.count
    end

    test "consumed_ms_by_partition sums totals per partition over the window" do
      travel_to Time.utc(2026, 4, 25, 21, 5, 30) do
        PartitionObservation.observe!(policy_name: "p", partition_key: "A", queue_lag_ms: 0, duration_ms: 100)
        PartitionObservation.observe!(policy_name: "p", partition_key: "A", queue_lag_ms: 0, duration_ms: 200)
        PartitionObservation.observe!(policy_name: "p", partition_key: "B", queue_lag_ms: 0, duration_ms: 50)

        result = PartitionObservation.consumed_ms_by_partition(
          policy_name: "p", partition_keys: %w[A B], window: 60
        )
        assert_equal 300, result.dig("A", :consumed_ms)
        assert_equal 2,   result.dig("A", :count)
        assert_equal 50,  result.dig("B", :consumed_ms)
        assert_equal 1,   result.dig("B", :count)
      end
    end

    test "consumed_ms_by_partition still sees the previous bucket right after a minute rolls" do
      # The PR #11 regression: observation at the tail of a minute,
      # query just after the wall clock crosses to the next minute.
      # The bucket value sits before "now - window" with the strict
      # filter; the +60s pad ensures it stays included.
      travel_to Time.utc(2026, 4, 25, 21, 1, 59, 500_000) do
        PartitionObservation.observe!(
          policy_name: "p", partition_key: "X",
          queue_lag_ms: 0, duration_ms: 1000
        )
      end
      travel_to Time.utc(2026, 4, 25, 21, 2, 0, 500_000) do
        result = PartitionObservation.consumed_ms_by_partition(
          policy_name: "p", partition_keys: %w[X], window: 60
        )
        assert_equal 1000, result.dig("X", :consumed_ms),
          "previous-minute bucket must remain visible 1s after the minute rolls"
      end
    end

    test "consumed_ms_by_partition excludes buckets older than window + pad" do
      travel_to Time.utc(2026, 4, 25, 21, 0, 0) do
        PartitionObservation.observe!(policy_name: "p", partition_key: "X", queue_lag_ms: 0, duration_ms: 1000)
      end
      travel_to Time.utc(2026, 4, 25, 21, 5, 0) do
        result = PartitionObservation.consumed_ms_by_partition(
          policy_name: "p", partition_keys: %w[X], window: 60
        )
        assert_empty result, "5-minute-old bucket should be outside any 60s window"
      end
    end

    test "consumed_ms_by_partition returns empty when partition_keys is empty" do
      assert_empty PartitionObservation.consumed_ms_by_partition(
        policy_name: "p", partition_keys: [], window: 60
      )
    end

    test "prune! deletes observations older than OBSERVATION_TTL" do
      travel_to Time.utc(2026, 4, 25, 12, 0, 0) do
        PartitionObservation.observe!(policy_name: "p", partition_key: "old", queue_lag_ms: 0, duration_ms: 1)
      end
      travel_to Time.utc(2026, 4, 25, 18, 0, 0) do
        PartitionObservation.observe!(policy_name: "p", partition_key: "fresh", queue_lag_ms: 0, duration_ms: 1)
        PartitionObservation.prune!
        assert_equal %w[fresh], PartitionObservation.pluck(:partition_key)
      end
    end

    # ───── Group 2: gate combination integration tests ─────

    class ThrottleFairJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :throttle, rate: 2, per: 60, partition_by: ->(ctx) { ctx[:tenant] }
        gate :fair_interleave
      end
      def perform(*); end
    end

    test "throttle + fair_interleave admits within rate per partition" do
      4.times { ThrottleFairJob.perform_later("A") }
      4.times { ThrottleFairJob.perform_later("B") }
      Tick.run(policy_name: ThrottleFairJob.resolved_dispatch_policy.name)

      grouped = StagedJob.admitted.group(:context).count.transform_keys { |c| (c || {})["tenant"] }
      assert_equal 2, grouped["A"]
      assert_equal 2, grouped["B"]
    end

    class GlobalCappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency, max: 5, partition_by: ->(ctx) { ctx[:tenant] }
        gate :global_cap,  max: 3
      end
      def perform(*); end
    end

    test "global_cap caps under per-partition concurrency" do
      4.times { GlobalCappedJob.perform_later("A") }
      4.times { GlobalCappedJob.perform_later("B") }
      Tick.run(policy_name: GlobalCappedJob.resolved_dispatch_policy.name)

      assert_equal 3, StagedJob.admitted.count, "global_cap should limit total to 3 even if concurrency would allow more"
    end

    class TimeWeightedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }, weight: :time
      end
      def perform(*); end
    end

    class TimeWeightedAdaptiveJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        round_robin_by ->(args) { args.first }, weight: :time
        gate :adaptive_concurrency,
             partition_by:  ->(ctx) { ctx[:tenant] },
             initial_max:   3,
             target_lag_ms: 500
      end
      def perform(*); end
    end

    test "weight: :time + adaptive_concurrency limits each partition by initial_max on first tick" do
      # Webhook recipe: time-weighted fetch + adaptive cap. With no
      # observations yet and identical pending counts, both partitions
      # should be admitted up to initial_max (3 each = 6 total), not
      # the whole batch_size.
      10.times { TimeWeightedAdaptiveJob.perform_later("A") }
      10.times { TimeWeightedAdaptiveJob.perform_later("B") }

      Tick.run(policy_name: TimeWeightedAdaptiveJob.resolved_dispatch_policy.name)

      grouped = StagedJob.admitted.group(:context).count.transform_keys { |c| (c || {})["tenant"] }
      assert_equal 3, grouped["A"]
      assert_equal 3, grouped["B"]
    end

    test "weight: :time admits a heavy partition less than a light one when consumed_ms differs" do
      # Fetch-layer fairness only: no admission-cap gate. The per-tick
      # quotas only diverge once batch_size is tight enough that the
      # weighted ratio matters; with the default 500 the entire
      # backlog fits in one tick and the difference is invisible.
      DispatchPolicy.config.batch_size = 40
      policy_name = TimeWeightedJob.resolved_dispatch_policy.name

      PartitionObservation.observe!(
        policy_name: policy_name, partition_key: "heavy",
        queue_lag_ms: 0, duration_ms: 20_000
      )

      30.times { TimeWeightedJob.perform_later("heavy") }
      30.times { TimeWeightedJob.perform_later("light") }
      Tick.run(policy_name: policy_name)

      grouped = StagedJob.admitted.group(:context).count.transform_keys { |c| (c || {})["tenant"] }
      assert_operator grouped["light"].to_i, :>, grouped["heavy"].to_i,
        "light should out-admit heavy when heavy has burned 20s (got #{grouped.inspect})"
    end

    # ───── Group 3: time-sensitive integration ─────

    test "weight: :time fairness survives a minute roll between observation and tick" do
      DispatchPolicy.config.batch_size = 40
      policy_name = TimeWeightedJob.resolved_dispatch_policy.name

      # Heavy partition burns 20s of compute in the previous minute.
      travel_to Time.utc(2026, 4, 25, 21, 1, 59) do
        PartitionObservation.observe!(
          policy_name: policy_name, partition_key: "heavy",
          queue_lag_ms: 0, duration_ms: 20_000
        )
      end

      # Tick happens 2 seconds later — the minute has rolled.
      travel_to Time.utc(2026, 4, 25, 21, 2, 1) do
        30.times { TimeWeightedJob.perform_later("heavy") }
        30.times { TimeWeightedJob.perform_later("light") }
        Tick.run(policy_name: policy_name)
      end

      grouped = StagedJob.admitted.group(:context).count.transform_keys { |c| (c || {})["tenant"] }
      assert_operator grouped["light"].to_i, :>, grouped["heavy"].to_i,
        "the previous-minute bucket must still drive the time weighting after the wall clock rolls (got #{grouped.inspect})"
    end
  end
end
