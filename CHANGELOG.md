# Changelog

## Unreleased

### Fixed
- Add a partial index on `dispatch_policy_staged_jobs(active_job_id)`
  scoped to in-flight rows (`completed_at IS NULL AND active_job_id
  IS NOT NULL`). Without it, the per-perform completion UPDATE
  (`mark_completed_by_active_job_id`) does a sequential scan on every
  finished job.
- Add two partial indexes used by the admin partition breakdown:
  `(policy_name) WHERE admitted_at IS NOT NULL AND completed_at IS NULL`
  for in-flight aggregations, and `(policy_name, completed_at) WHERE
  completed_at IS NOT NULL` for completed-in-window aggregations. The
  pre-existing `(completed_at) WHERE completed_at IS NOT NULL` index
  is left in place for now — drop it in a follow-up after confirming
  in production that nothing else depends on it.

Existing installs need to run the new migrations
(`bundle exec rails dispatch_policy:install:migrations` then
`db:migrate`).

## 0.2.0

### Added
- `round_robin_by` supports `weight: :time` to balance per-tick quanta
  by recent perform compute time instead of by request count (#3).
- GitHub Actions CI matrix covering Ruby 3.4 and Rails 7.2 / 8.1 (#5).
- Integration tests for gate combinations and throttle bucket
  boundaries (#12).
- Resilience tests covering failure paths and dedupe state transitions
  (#13).
- `bin/release` wrapper around `rake release` (#2).

### Changed
- Admin partition breakdown caps its aggregations to keep the page
  responsive on policies with many partitions (#9).
- Admin pending list no longer loads the `arguments` jsonb column
  (#6).

### Fixed
- Admission is reverted when the underlying adapter silently declines
  to enqueue, so the staged row doesn't stay marked as admitted (#14).
- `consumed_ms_by_partition` window is padded to survive
  minute-boundary races in the time-weighted round-robin fetch (#11).
- ThrottleBucket row locks are taken in a deterministic key order to
  remove a deadlock window when multiple ticks contend on the same
  set of partitions (#8).

### Removed
- Stale custom `InstallGenerator` — the engine's migration generator
  is the supported install path (#7).

## 0.1.0

Initial release.

- Rails engine + ActiveJob integration (`DispatchPolicy::Dispatchable`).
- Gates: `:throttle`, `:concurrency`, `:global_cap`, `:fair_interleave`, `:adaptive_concurrency`.
- Staged jobs with dedupe, round-robin fairness, per-partition counters, and throttle buckets.
- Admin UI (Chart.js + Turbo) with watched partitions, sparklines, and EWMA queue-lag charts.
- PostgreSQL required (uses `FOR UPDATE SKIP LOCKED`, `ON CONFLICT`, and `jsonb`).
- Experimental — being trialed on pulso.run.
