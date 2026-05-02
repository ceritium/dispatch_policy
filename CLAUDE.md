# dispatch_policy — guide for future sessions

Minimal cheat sheet to pick up the project without rehydrating the
whole memory. The truth lives in the code, git log, and README; this
file is **only** what is NOT derivable by reading those.

## What it is

Rails gem that acts as **per-partition admission control** over
ActiveJob, persisted in Postgres. It intercepts `perform_later`,
stages the job in an intermediate table, and a periodic *tick loop*
decides how many jobs to release to the real adapter (`good_job` /
`solid_queue`) according to declared gates (`throttle`, `concurrency`).

See `README.md` for the API and examples.

## Status

v0.1 (on master). The whole main flow is implemented and tested.
What's pending lives in `IDEAS.md` with the rationale.

119 tests / 263 assertions. `bundle exec rake test` from the root.

## Architecture — 5 tables

```
dispatch_policy_staged_jobs                   intercepted jobs awaiting admission
dispatch_policy_partitions                    one row per (policy, partition_key)
                                              — gate_state (token bucket), shard,
                                              last_checked_at, next_eligible_at, …
dispatch_policy_inflight_jobs                 admitted jobs currently running
                                              — heartbeat_at refreshed by a thread
dispatch_policy_tick_samples                  one row per Tick.run for metrics
dispatch_policy_adaptive_concurrency_stats    AIMD-tuned current_max + EWMA lag
                                              per partition for adaptive gates
```

## Flow

1. `MyJob.perform_later(args)` → `JobExtension.around_enqueue_for` →
   `Repository.stage!` (INSERT staged + UPSERT partition with
   refreshed ctx and shard pinned-on-first-write).
2. A `DispatchTickLoopJob` runs `TickLoop.run(policy_name:, shard:)`.
3. Each Tick: `Repository.claim_partitions` → for each partition,
   `Pipeline.call(ctx, partition)` → **a single TX** doing
   `Repository.claim_staged_jobs!` (DELETE … RETURNING) +
   pre-INSERT in `inflight_jobs` + `Forwarder.dispatch` (re-enqueue
   to the adapter under `Bypass.with`). The PG-backed adapter shares
   the connection, so its INSERT joins the same TX.
4. The adapter's worker runs the job: `InflightTracker.track`
   (around_perform) idempotently INSERTs into `inflight_jobs`,
   spawns a heartbeat thread, and on `ensure` cancels it and DELETEs.

## Invariants — don't break without thinking

- **`partition_key` identifies a partition; `shard` is routing
  metadata.** The shard is pinned on first write
  (`COALESCE(EXCLUDED.shard, partitions.shard)`) so partitions don't
  jump between tick workers.
- **`partition_by` is policy-level and required.** A single
  declaration `partition_by ->(ctx) { … }` in the policy block. The
  staged_job's `partition_key` and the concurrency gate's
  `inflight_partition_key` share that same canonical value → no gate
  suffers scope dilution. **There is no per-gate `partition_by:`
  anymore.** If omitted, `Policy#validate!` raises
  `InvalidPolicy: partition_by required`. For genuinely different
  per-gate scopes, use separate policies.
- **Gates are NOT required.** A policy with `partition_by` and no
  gates is valid — the pipeline returns `admit_count = max_budget`
  and the in-tick fairness reorder (decay + fair_share) still
  applies. Useful for "balance N tenants without rate-limiting any
  of them". Without a concurrency gate the job doesn't need
  `dispatch_policy_inflight_tracking` either.
- **`partitions.context` is refreshed on every `perform_later`** via
  UPSERT. Gates read that ctx, NOT `staged_jobs.context` (which is
  historical). This lets a change in the host DB (e.g. new
  `max_per_account`) take effect on the next enqueue.
- **`shard_by` must be ≥ as coarse as the most restrictive throttle's
  scope.** If not, the bucket duplicates across shards and the
  effective rate becomes `rate × N_shards`.
- **`BulkEnqueue.perform_all_later` checks `Bypass.active?`** and
  delegates to `super` when active. Without it, the call from
  `Forwarder.dispatch` (deserialize + `perform_all_later` under
  Bypass) re-staged in an infinite loop. The fix lives in
  `job_extension.rb`; a regression test in
  `test/integration/tick_atomic_test.rb`
  (`test_full_tick_with_kwargs_does_not_re_stage`) fails if you
  remove it.
- **`JobExtension.ensure_arguments_materialized!(job)`** is called
  before reading `job.arguments` in both the single and bulk paths.
  Reason: `klass.deserialize(payload)` only sets
  `@serialized_arguments`; the public `arguments` getter is a plain
  `attr_accessor` returning `@arguments = []` until `perform_now`
  triggers private materialization. Without this defense the context
  proc receives `[]` and falls back to its defaults.
- **`Forwarder.dispatch` runs INSIDE the admission TX.** The adapter
  (good_job / solid_queue) uses `ActiveRecord::Base.connection`, so
  its INSERT into `good_jobs` / `solid_queue_jobs` joins the same
  transaction as the DELETE from `staged_jobs` and the INSERT into
  `inflight_jobs`. Any exception (deserialize, adapter, network)
  rolls everything back atomically — no loss window between admission
  COMMIT and adapter enqueue. **Do not reintroduce `unclaim!` or
  `preinserted_inflight_ids`**: TX rollback covers that. If you ever
  support a non-PG adapter, think first about how to keep
  at-least-once without this invariant.
- **Non-PG adapter = warn at boot, no hard-fail.** The railtie calls
  `DispatchPolicy.warn_unsupported_adapter` in `after_initialize`.
  If the host runs Sidekiq/Resque, a warning explains atomicity is
  lost. Deliberate: a custom PG-backed adapter (not detected) can
  still work, and we don't want to break its deploy.
- **`config.database_role`**: for Rails multi-DB (e.g. solid_queue
  with a separate DB), sets the role the admission TX is opened
  against. `Repository.with_connection` wraps the TX in
  `connected_to(role:)` when set. Staging tables and the adapter's
  table must live in the same DB for atomicity to hold.
- **Every admitted job creates a row in `inflight_jobs`**, whether or
  not there's a concurrency gate. The key is always
  `policy.partition_for(ctx)` (same canonical scope as the staged
  partition_key), since `partition_by` is policy-level. The UI counts
  by `policy_name` and always reports a real value.
- **`:adaptive_concurrency` updates `current_max` in a single SQL
  statement.** The UPDATE in `Repository.adaptive_record!` uses the
  POST-update `ewma_latency_ms` value in its CASE expression — so
  one observation can simultaneously raise the EWMA AND trigger a
  shrink against the new value. Concurrent workers can call it in
  any order without read-modify-write races. The gate also seeds
  the row on every evaluate AND every record_observation (idempotent
  ON CONFLICT DO NOTHING) to keep both paths self-sufficient.
- **`:adaptive_concurrency` safety valve.** When `in_flight == 0`
  the gate floors `remaining` at `initial_max` regardless of what
  `current_max` says. Reason: AIMD can shrink the cap during a slow
  burst; if the partition then idles, no observations fire to grow
  it back. Without the floor a partition fossilizes at `min`.
- **Adaptive's feedback signal is `queue_lag = perform_start -
  admitted_at`.** Measured in `InflightTracker.track` BEFORE
  `block.call` so perform duration doesn't pollute the signal.
  `admitted_at` is read from the inflight_jobs row pre-INSERTed by
  the Tick — that timestamp is the canonical "moment of admission".
  If the lookup fails (row missing, parse error) the observation is
  recorded with `queue_lag_ms = 0` — the cap can still grow.
- **In-tick fairness = ordering + cap, NOT mixed with selection.**
  `claim_partitions` still orders by `last_checked_at NULLS FIRST,
  id` (anti-stagnation: each partition with pending is processed
  every ⌈N/B⌉ ticks). Once claimed, the Tick reorders them in
  memory by `decayed_admits ASC` (EWMA, default `half_life = 60s`)
  and applies `fair_share = ceil(tick_cap / N)` as the per-partition
  ceiling. **Do not reintroduce `decayed_admits` into the SELECT FOR
  UPDATE's ORDER BY** — that breaks the anti-stagnation guarantee
  when there are > batch_size fresh partitions.
- **The global tick cap wins over the anti-stagnation per-tick
  floor.** If `tick_admission_budget < N_claimed`, some partitions
  admit 0 this tick. We do NOT force a floor of 1 (that would break
  the cap). Fairness comes from claim_partitions: their
  `last_checked_at` is bumped on claim, so the next tick puts them
  at the front.
- **The decay update happens inside the admit TX.** In
  `record_partition_admit!`, when `half_life_seconds` is set, the
  UPDATE includes `decayed_admits = decayed_admits * exp(-Δt/τ) +
  admitted` and `decayed_admits_at = now()`. Same row lock we already
  hold. `bulk_record_partition_denies!` does NOT touch the decay
  (no admission means no increment).
- **`claim_staged_jobs!` requires `limit > 0`** (it's now the
  admit-only path). The pure-deny path goes through
  `Repository.bulk_record_partition_denies!`: the Tick accumulates
  all denies in the batch and flushes them with a single
  `UPDATE…FROM(VALUES…)` at the end, instead of N per-partition
  statements. Per-row equivalence (no cross-partition aggregation)
  preserves correctness. The critical part is not losing the
  `gate_state || patch` (jsonb merge) — an integration test pins
  the case "the patch must not overwrite pre-existing keys".

## Things whose break breaks the UI

- The layout reuses Turbo (re-added after the meta-refresh
  collision). The user added an auto-refresh picker in
  sessionStorage. If you mess with `Turbo.visit`, remember the
  vanilla `setTimeout` can race the visit — there's an in-flight
  guard for that.
- `lib/` does NOT autoload in Rails dev. Any change under
  `lib/dispatch_policy/*` requires restarting foreman.
- Foreman defaults `PORT=5000`. On macOS port 5000 is AirPlay → 403.
  The Procfile pins `-p 3000`.

## How to develop

```bash
# Start the dummy app (web + worker + tick) with foreman
bin/dummy setup good_job        # creates the DB and migrates
DUMMY_ADAPTER=good_job bundle exec foreman start

# Useful endpoints
http://localhost:3000/                       # forms to enqueue
http://localhost:3000/dispatch_policy        # dashboard

# Tests
bundle exec rake test                        # 101 runs / 225 asserts

# When you add a column or table:
#   1. Edit db/migrate/20260501000001_create_dispatch_policy_tables.rb
#   2. Edit lib/generators/.../create_dispatch_policy_tables.rb.tt
#   3. For the live dummy app, ALTER TABLE manually (no incremental
#      migrations because v0.1 ships a single migration)
#   4. test/integration/repository_test.rb#schema_present? detects
#      drift via known columns; add the new one to the check.
```

## Useful debug queries

```sql
-- Distribution of partitions per policy/shard with pending and lifetime
SELECT policy_name, shard, status, count(*) AS partitions,
       sum(pending_count) AS pending, sum(total_admitted) AS lifetime
FROM dispatch_policy_partitions
GROUP BY policy_name, shard, status
ORDER BY pending DESC;

-- Partitions currently in backoff
SELECT policy_name, partition_key,
       gate_state -> 'throttle' ->> 'tokens' AS tokens,
       (next_eligible_at - now()) AS time_left
FROM dispatch_policy_partitions
WHERE next_eligible_at > now();

-- Tick samples for the last minute
SELECT policy_name, count(*) AS ticks, sum(jobs_admitted) AS admitted,
       avg(duration_ms)::int AS avg_ms
FROM dispatch_policy_tick_samples
WHERE sampled_at > now() - interval '1 minute'
GROUP BY policy_name;
```

## What's in `IDEAS.md`

Detected items, deferred with their rationale. Read it before
proposing a "new" improvement — likely it's already noted.

Currently:
- More aggressive sweeper for orphan partitions with `pending=0`
- Revisit the coupling between `inflight_heartbeat_interval`,
  `inflight_stale_after` and `sweep_every_ticks`

## Repo conventions

- Unit tests in `test/unit/`, integration (with Postgres) in
  `test/integration/`. Integration tests skip when no DB is
  available.
- Commit messages in English; the body explains the **why**, not
  just the what. Co-Author tag at the end.
- The user edits the dummy app (stress jobs, layout) between my
  commits — respect those modifications.
