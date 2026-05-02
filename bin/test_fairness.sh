#!/usr/bin/env bash
# Manual fairness verification for the dummy app.
#
# Prerequisites:
#   1. The dummy app is running with the fairness-intra-tick branch
#      (foreman restart required after pulling: lib/ doesn't autoreload).
#   2. dispatch_policy_dummy DB has the decayed_admits / decayed_admits_at
#      columns (already added by the manual ALTER TABLE).
#
# What this does:
#   - Truncates the gem's tables (does NOT touch good_jobs).
#   - Sets a tight tick_admission_budget so fairness is observable.
#   - Stages 1000 jobs to "hot" and 10 to "cold" via the web UI.
#   - Prints the partition ledger every second for ~10s.
#
# Expected: "cold" drains in a handful of ticks even though "hot"
# has 100x more pending. decayed_admits for both stays roughly equal
# in steady state.

set -e
DB=${DB:-dispatch_policy_dummy}

echo "=== Truncate ==="
psql -d "$DB" -c "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, dispatch_policy_inflight_jobs, dispatch_policy_tick_samples;"

echo ""
echo "=== Stage 1000 hot ==="
curl -s -X POST -d "attrs[endpoint]=hot&attrs[rate]=10000&count=1000" http://localhost:3000/enqueue_many/high_throttle > /dev/null
echo "  staged"

echo ""
echo "=== Stage 10 cold ==="
curl -s -X POST -d "attrs[endpoint]=cold&attrs[rate]=10000&count=10" http://localhost:3000/enqueue_many/high_throttle > /dev/null
echo "  staged"

echo ""
echo "=== Watch partitions for 10s ==="
for i in 1 2 3 4 5 6 7 8 9 10; do
  echo "--- t=${i}s ---"
  psql -d "$DB" -c "
    SELECT partition_key,
           pending_count,
           total_admitted,
           round(decayed_admits::numeric, 2) AS decay
      FROM dispatch_policy_partitions
     WHERE policy_name = 'high_throttle'
     ORDER BY partition_key;
  " --tuples-only --no-align --field-separator='	' --pset border=0
  sleep 1
done
