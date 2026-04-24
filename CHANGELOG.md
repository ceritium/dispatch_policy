# Changelog

## 0.1.0

Initial release.

- Rails engine + ActiveJob integration (`DispatchPolicy::Dispatchable`).
- Gates: `:throttle`, `:concurrency`, `:global_cap`, `:fair_interleave`, `:adaptive_concurrency`.
- Staged jobs with dedupe, round-robin fairness, per-partition counters, and throttle buckets.
- Admin UI (Chart.js + Turbo) with watched partitions, sparklines, and EWMA queue-lag charts.
- PostgreSQL required (uses `FOR UPDATE SKIP LOCKED`, `ON CONFLICT`, and `jsonb`).
- Experimental — being trialed on pulso.run.
