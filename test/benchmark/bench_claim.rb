# frozen_string_literal: true

# claim_partitions performance at high partition cardinality.
#
# This is the SELECT … FOR UPDATE SKIP LOCKED + UPDATE … RETURNING
# query at the head of every Tick. Its cost dominates as the
# partitions table grows, because the planner has to walk the
# (policy, status, next_eligible_at, last_checked_at) index.
#
# We measure with all partitions eligible (next_eligible_at NULL)
# and the same partition_batch_size = 50.

require_relative "bench_helper"

Bench.connect!
Bench.recreate_schema!

Bench.section("Repository.claim_partitions (limit=50)") do |sec|
  [100, 1_000, 10_000, 100_000].each do |total|
    skip_msg = nil
    if total == 100_000 && ENV["BIG_SCALES"] != "1"
      skip_msg = "skipped (set BIG_SCALES=1 to include)"
    end

    if skip_msg
      sec.row("#{total} partitions in DB", 0.0, status: skip_msg)
      next
    end

    Bench.truncate!
    Bench.seed!("bench_claim", total, staged_per_partition: 1)

    median = Bench.measure_median_ms(runs: 5) do
      ActiveRecord::Base.connection.transaction(requires_new: true) do
        DispatchPolicy::Repository.claim_partitions(
          policy_name: "bench_claim",
          limit:       50
        )
        # Roll back so the next run has the same starting state (no
        # last_checked_at bumped). claim_partitions uses
        # FOR UPDATE SKIP LOCKED which returns its lock at TX commit.
        raise ActiveRecord::Rollback
      end
    end
    sec.row("#{total} partitions in DB", median)
  end
end

Bench.section("claim_partitions with shard filter") do |sec|
  Bench.truncate!
  # 10k partitions split across 4 shards.
  4.times do |s|
    2_500.times do |i|
      ActiveRecord::Base.connection.execute(<<~SQL)
        INSERT INTO dispatch_policy_partitions
          (policy_name, partition_key, queue_name, shard, status,
           pending_count, total_admitted, context, gate_state, decayed_admits,
           created_at, updated_at)
        VALUES ('bench_shard', 's#{s}-p#{i}', NULL, 'shard-#{s}', 'active',
                1, 0, '{}'::jsonb, '{}'::jsonb, 0.0, now(), now())
      SQL
    end
  end

  median = Bench.measure_median_ms(runs: 5) do
    ActiveRecord::Base.connection.transaction(requires_new: true) do
      DispatchPolicy::Repository.claim_partitions(
        policy_name: "bench_shard",
        shard:       "shard-2",
        limit:       50
      )
      raise ActiveRecord::Rollback
    end
  end
  sec.row("10k partitions, shard='shard-2'", median, total_partitions: 10_000)
end

Bench.print_report
