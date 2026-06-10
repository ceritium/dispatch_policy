# Changelog

## Unreleased

### Upgrade notes
- **New table `dispatch_policy_policy_settings`.** Required by the
  policy-level pause fix below. New installs get it from the updated
  install generator. **Existing installs must add it** — the gem ships a
  single migration, so either re-copy the migration via
  `rails dispatch_policy:install:migrations` (or hand-apply) or run:

  ```ruby
  create_table :dispatch_policy_policy_settings do |t|
    t.string  :policy_name, null: false
    t.boolean :paused,      null: false, default: false
    t.timestamps
  end
  add_index :dispatch_policy_policy_settings, :policy_name,
            unique: true, name: "idx_dp_policy_settings_lookup"
  ```

  Until the table exists, the tick's `claim_partitions` raises
  `PG::UndefinedTable`. One row per policy holds its pause flag; it's the
  policy-wide source of truth `claim_partitions` consults.

### Added
- Policy-level **pause** now actually holds the whole policy. The pause
  flag lives in the new `dispatch_policy_policy_settings` table and is
  honored by `claim_partitions`, so it also stops partitions that first
  appear *after* the pause — previously `pause` only flipped the `status`
  of partition rows that existed at click time, and a tenant's first
  enqueue afterwards created an `active` partition the next tick admitted.
  The per-partition `status` update is kept for the partitions-index
  display; `resume` clears the flag.

### Fixed
- **Multi-DB (`config.database_role`) is now honored everywhere.** It was
  only applied at the three admission-TX boundaries (`Tick`,
  `ManualAdmission`), leaving staging, partition claim, inflight
  counts/tracking, sweeps and dashboard reads on the default writing role.
  Under a separate queue DB (e.g. `solid_queue`) with the gem tables
  there, staging wrote one DB while the tick read another — silent job
  loss — and the concurrency gate counted inflight rows in a different DB
  than the tracker wrote them to. Every public `Repository` method now
  opens inside `connected_to(role:)`; `InflightTracker`'s direct access
  (lookup + heartbeat thread) is routed too.
- **A policy may declare each gate type at most once.** Two gates of the
  same type shared a single `gate_state` key (both throttles wrote
  `gate_state["throttle"]`), so the merged patch kept only the last gate's
  bucket and the other then saw a permanently full bucket — silently
  defeating the stricter limit (the classic 10/min + 600/hour idiom).
  `Policy#validate!` now raises `InvalidPolicy`; use separate policies for
  multi-window limits.
- **Bulk `perform_all_later` correctness.** A job whose declared policy
  wasn't registered was silently dropped (neither staged nor sent to the
  adapter); jobs were marked `successfully_enqueued` before the INSERT
  committed; and the bulk path ignored `bypass_retries`. It now mirrors
  the single path: unstageable jobs fall through to the adapter, the
  enqueued flag is set only after `stage_many!` returns, and retries on a
  `:bypass` policy skip staging.
- **`ManualAdmission.force!` pre-inserts inflight rows** in the same
  transaction as the claim, like the Tick. Without it the concurrency
  gate under-counted force-admitted jobs (UI admit/drain) until each one
  started performing — an over-admission window proportional to the
  backlog drained.
- **Inflight rows are reaped when a job is discarded before performing.**
  `discard_on ActiveJob::DeserializationError` (and any discard) fires
  during argument deserialization, before `around_perform`, so
  `InflightTracker.track`'s `ensure` never ran and the Tick's pre-inserted
  row sat until the `inflight_queued_stale_after` sweeper (1h), holding a
  concurrency slot. The railtie now subscribes to `discard.active_job` and
  deletes the row by `active_job_id`.
- **`throttle` no longer busy-loops on a zero/nil rate.** A `rate` of `0`
  or `nil` (e.g. a paused tenant) denied with a NULL `retry_after`, which
  left the partition immediately eligible — re-claimed and re-evaluated
  every tick — and clobbered any existing backoff. It now backs off one
  `per` window, and `bulk_record_partition_denies!` preserves the existing
  `next_eligible_at` when `retry_after` is NULL instead of nulling it.
- **`throttle` rate is read as `Float`.** A fractional rate (e.g. `2.5`)
  kept its fractional part instead of truncating every refill (systematic
  under-admission), and a sub-unit rate (`rate: 0.5`) accumulates a whole
  token and admits instead of truncating to `0` and denying forever.
- **`adaptive_concurrency` validates its tuning knobs.** Out-of-range
  values silently inverted the AIMD loop: `ewma_alpha: 0` froze the EWMA
  at its seed so the cap grew unbounded, and a decrease factor `>= 1`
  turned the multiplicative *decrease* into a positive-feedback *increase*
  under failure/overload. The constructor now requires
  `0 < ewma_alpha <= 1` and `0 < failure/overload_decrease_factor < 1`.
- **`partitions#admit` bounds its count.** An unbounded `count` forced a
  single `DELETE…RETURNING` + dispatch of the whole backlog in one
  transaction (bypassing the batching/cap that `drain` uses), and a
  non-numeric value 500'd. It's now clamped to `[1, 10_000]` with a
  fallback to `1`.
- **Forged timestamp pagination cursors no longer 500.** A non-parseable
  string on a `stale`/`recent` sort bound into a timestamp column and
  raised `invalid input syntax for type timestamp`. `CursorPagination`
  now requires a parseable ISO8601 value for timestamp sorts, falling back
  to the first page otherwise.

## 0.4.3

### Fixed
- The `throttle` gate now charges its token bucket for the number of jobs
  **actually admitted**, not for the optimistic `allowed` it computes at
  evaluate time. The deduction moved from `#evaluate` to the `#consume`
  hook (run after the staging DELETE, via `Pipeline.settle`), so the
  bucket is no longer over-charged — and the effective rate no longer
  drifts below the configured one — when fewer jobs are admitted than
  allowed: future-scheduled rows skipped by the `scheduled_at <= now()`
  filter, a downstream `concurrency` gate capping `admit_count`, or rows a
  concurrent tick claimed under `SKIP LOCKED`.
- Inflight rows for jobs that were admitted but have **not started
  performing yet** (still waiting in the adapter's queue) are no longer
  reaped at `inflight_stale_after`. Their heartbeat thread only starts in
  `around_perform`, so under a deep adapter backlog the sweeper used to
  delete still-valid admissions, making the concurrency gate under-count
  and over-admit. `sweep_stale_inflight!` is now two-tier: rows
  heartbeated past admission reap at `inflight_stale_after`; never-started
  rows reap only past the new, generous `config.inflight_queued_stale_after`
  (1 hour default).
- `InflightTracker` now applies the same `job.queue_name || policy.queue_name`
  fallback at perform time that the staging path uses, so a policy whose
  `partition_by`/`shard_by` reads `queue_name` derives the same
  `partition_key` at admission and at perform (otherwise the inflight row
  and adaptive observations landed under the wrong scope).
- `CursorPagination` rejects cursors whose value isn't a scalar or whose
  id isn't an integer (the cursor is an attacker-controllable query
  param), and ignores a value whose type can't compare against the sort
  column instead of raising a `PG` error (a forged numeric value on a
  timestamp sort). Falls back to the first page.
- `PolicyDSL#tick_admission_budget(nil)` / `#admission_batch_size(nil)` are
  no-ops that defer to config instead of raising in `Integer(nil)`,
  matching how `fairness(half_life:)` already guards nil.

### Changed
- The admin UI's dashboard and policies index collapse their per-policy
  `N+1` query loops into grouped `Repository` methods
  (`tick_summaries_by_policy`, `top_denied_reason_by_policy`,
  `partition_round_trip_stats_by_policy`, `partition_counts_by_policy`),
  one query each instead of several per policy.

### Added
- `config.inflight_queued_stale_after` (default 1 hour) — the sweep cutoff
  for inflight rows admitted but never started. Raise it if your adapter
  backlog can exceed an hour.

### Removed
- The broken, unused `Partition.stale_inactive` scope — it filtered on an
  `in_flight_count` column dropped back in 0.3.0, so any call raised
  `PG::UndefinedColumn`. The real partition GC is
  `Repository.sweep_inactive_partitions!`.

## 0.4.2

### Fixed
- The engine UI's **"admit"** and **"drain"** buttons now claim and
  forward jobs inside a single transaction, matching the Tick's
  at-least-once guarantee. They previously ran
  `Repository.claim_staged_jobs!` (a `DELETE … RETURNING` that
  autocommits on its own) and then a bare `Forwarder.dispatch` in a
  separate statement — so if the forward raised (deserialize, adapter,
  network), the staged rows were already deleted and the jobs were
  lost. The atomic primitive now lives in
  `DispatchPolicy::ManualAdmission.force!` and both controller actions
  delegate to it.
- The same UI paths now regenerate `active_job_id` per row before the
  adapter handoff, as the Tick admission path already did in 0.4.1.
  Without it the manual buttons could raise
  `ActiveRecord::RecordNotUnique` against a residual adapter row from a
  previous admission — which, combined with the missing transaction,
  both 500'd the request and lost the staged rows.
- Fixes a latent `NoMethodError` in the `admit` action: the old
  `rows.size - Forwarder.dispatch(rows).size` raised when the claim
  came back empty (`dispatch` returns `nil`) and otherwise reported a
  misleading forwarded count.
- Tick pass-2 budget redistribution no longer double-spends the
  `throttle` token bucket. When `tick_admission_budget` is set, pass-2
  re-evaluates a partition's gates against the in-memory partition
  hash; the throttle bucket lives in `partitions.gate_state`, which was
  persisted to the DB in pass-1 but never mirrored back in memory. So
  pass-2 re-read the pre-pass-1 token count, admitted again from a full
  bucket (above the configured rate), and persisted a patch computed
  off the stale base — silently dropping pass-1's consumption, so the
  effective rate drifted upward tick after tick. The committed
  `gate_state` patch is now shallow-merged back onto the in-memory
  partition after each admit. (`concurrency` / `adaptive_concurrency`
  were unaffected — they re-read their counts from the DB on every
  evaluate.)

## 0.4.1

### Fixed
- Admission now regenerates `active_job_id` for each row before
  pre-inserting `dispatch_policy_inflight_jobs` and handing the job
  to the adapter. Adapters that use `active_job_id` as the PK of
  their jobs table (`good_job`, `solid_queue`) would otherwise raise
  `ActiveRecord::RecordNotUnique` on `good_jobs_pkey` /
  `solid_queue_jobs_pkey` when a residual row from a previous
  admission of the same staged job still existed — most commonly a
  retry-restage (default `retry_strategy: :restage`) whose original
  adapter row had not been finalized yet. The collision rolled back
  the entire admission TX, the staged row returned, and the next
  tick re-collided in a loop. The staged-side identity is
  `staged_jobs.id`; the active_job_id only needs to be unique at
  adapter-insert time.
- `record_partition_admit!` clamps the EWMA decay exponent at -700
  so `exp()` no longer raises `value out of range: underflow` when a
  partition has been idle for many half-lives. Postgres throws this
  error around `exp(-746)` on double precision, and a partition that
  sat idle long enough (e.g. a few weeks with `half_life = 60s`)
  produced a Δt/τ ratio past that threshold; the broken UPDATE rolled
  back the whole admission TX every tick, so the partition could
  never drain again. -700 still yields a finite ~9.86e-305, which is
  effectively zero for the EWMA.

## 0.4.0

### Added
- Admin UI dark mode with an auto / light / dark selector in the
  header that persists across pages.
- Conceptual logo (chevrons + gate + admitted dot across 3
  partitions), surfaced in the admin header and README. Theme-aware
  lockup keeps the wordmark readable on both light and dark
  backgrounds.
- Vendored Turbo, served by the engine, so the admin UI no longer
  depends on the host app shipping Turbo itself.
- `screenshots` Rake task that regenerates all README screenshots
  from the dummy app instead of importing static assets.

### Changed
- Slimmer brand bundle in the gem: only the two masters actually
  used are shipped; the full art set stays in `arts/` for source.
- README install section drops the obsolete v2-branch banner — the
  gem now installs cleanly from RubyGems.

### Fixed
- Dummy app opts into the gem's `db/migrate` path so its setup
  picks up new tables without manual copying.

## 0.3.0

### Added
- TX-atomic admission: the DELETE on `staged_jobs`, the pre-INSERT
  in `inflight_jobs` and the adapter handoff (`good_job` /
  `solid_queue`) all run inside the same transaction, so any failure
  rolls everything back with no loss window between admission COMMIT
  and adapter enqueue.
- `:adaptive_concurrency` gate that auto-tunes per-partition
  `current_max` via AIMD against an EWMA of `queue_lag` (time from
  admission to perform start), with a safety valve that floors
  `remaining` at `initial_max` when `in_flight == 0` so idle
  partitions can recover after a shrink.
- In-tick fairness layer: claimed partitions are reordered by
  `decayed_admits` (EWMA, default `half_life = 60s`) and capped by
  `fair_share = ceil(tick_cap / N)`. Composes with
  `:adaptive_concurrency` — fairness writes
  `partitions.decayed_admits`, adaptive writes
  `dispatch_policy_adaptive_concurrency_stats.current_max`, no
  shared locks.
- `shard_by` to split a policy's partitions across parallel tick
  loops; the shard is pinned on first write so partitions don't jump
  between tick workers.
- Policy-level `partition_by`: a single canonical scope shared by
  the staged job's `partition_key` and the concurrency gate's
  `inflight_partition_key`, so no gate suffers scope dilution.
- Gates are no longer required — a policy with `partition_by` and
  no gates is valid and still benefits from in-tick fairness.
- `dispatch_policy_inflight_jobs` is populated for every admitted
  job (not only concurrency-gated ones), with a heartbeat thread
  refreshing `heartbeat_at` during perform.
- Bulk handoff via `ActiveJob.perform_all_later` and bulk-flush of
  deny-path partition state at the end of a tick (single
  `UPDATE…FROM(VALUES…)` instead of N per-partition statements).
- Per-tick metrics layer (`dispatch_policy_tick_samples`) feeding
  the admin UI: throughput, P50/P95 round-trip ages, capacity
  headroom, pending trend, fail %, and operator hints.
- Admin UI improvements: cursor-based pagination of `/partitions`,
  sort + only-pending filter, auto-refresh control (off / 2s / 5s /
  10s) via Turbo Drive, per-partition and per-policy Drain action,
  redesigned dummy demo page with cards + storm controls.
- `config.enabled` master switch for cutovers.
- `TickLoop` `busy_pause` to throttle busy iterations.
- `bin/release` wrapper around `rake release`.
- Manual benchmark suite, plus a real-adapter end-to-end bench
  covering `good_job` and `solid_queue`.

### Changed
- **Breaking:** `partition_by` is policy-level only. Per-gate
  `partition_by:` was removed; if omitted, `Policy#validate!` raises
  `InvalidPolicy: partition_by required`. For different per-gate
  scopes, use separate policies.
- `partitions.context` is refreshed on every `perform_later` via
  UPSERT, so changes in the host DB take effect on the next enqueue
  without redeploys. Gates read this ctx, not the historical
  `staged_jobs.context`.
- Tick-claim ordering kept at `last_checked_at NULLS FIRST, id`
  (anti-stagnation): each partition with pending is processed every
  ⌈N/B⌉ ticks. Fairness reorder happens after the claim, in memory.
- Non-PG adapters now warn at boot (`warn_unsupported_adapter`)
  instead of hard-failing — a custom PG-backed adapter still works.
- `config.database_role` lets the admission TX target a specific
  Rails multi-DB role (e.g. `solid_queue` on a separate DB).

### Fixed
- `BulkEnqueue.perform_all_later` checks `Bypass.active?` and
  delegates to `super` when active, breaking an infinite re-staging
  loop on the deserialize + `perform_all_later` path under Bypass.
- `JobExtension.ensure_arguments_materialized!` is called before
  reading `job.arguments` in both single and bulk paths — previously
  the public `arguments` getter returned `[]` for deserialised jobs
  until `perform_now` triggered private materialization, so the
  context proc fell back to its defaults.
- `:adaptive_concurrency` updates `current_max` in a single SQL
  statement that uses the post-update `ewma_latency_ms` value in
  its CASE expression, removing read-modify-write races between
  concurrent workers.
- Adaptive's feedback signal is measured in `InflightTracker.track`
  before `block.call` so perform duration doesn't pollute the
  `queue_lag` signal.
- Heartbeat thread refreshes `inflight_jobs.heartbeat_at` during
  perform so long-running jobs aren't reaped as stale.
- Deny-only ticks persist `next_eligible_at`.
- Tick samples query no longer depends on `date_bin` (works on
  Postgres 13).
- Admin UI preserves scroll position on auto-refresh, and skips
  auto-refresh while a Turbo visit is in flight.
- P95/P50 round-trip ages were inverted in the metrics view.
- Railtie no longer auto-merges the gem's `db/migrate` into the
  host's paths.
- "Pending is growing" hint silenced when the backlog has drained.

### Removed
- Per-gate `partition_by:` declarations (see Changed).
- Denormalised `partitions.in_flight_count` counter — `inflight_jobs`
  is the source of truth.
- `unclaim!` / `preinserted_inflight_ids` — TX rollback covers the
  failure case.

## 0.1.0

Initial release.

- Rails engine + ActiveJob integration intercepting `perform_later`
  via `JobExtension`.
- Gates: `:throttle`, `:concurrency`.
- Staged jobs admitted by a periodic tick loop, per-partition
  counters, and token-bucket throttle state.
- Admin UI showing partitions, pending counts, and recent ticks.
- PostgreSQL required (uses `FOR UPDATE SKIP LOCKED`, `ON CONFLICT`,
  and `jsonb`).
