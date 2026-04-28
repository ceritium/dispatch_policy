# Architecture notes

## Why dispatch_policy requires a PG-backed adapter

The single most important invariant in dispatch_policy is that
**admission and adapter enqueue happen in the same PostgreSQL
transaction**. That is what gives the failure-mode table in the README
its strong guarantees: any error — gate raise, adapter raise, polite
adapter decline, process crash — rolls back atomically. Rows revert to
`pending`, counters never drift, no half-enqueued state exists.

This invariant only holds when the adapter's job storage lives in the
same database connection as `dispatch_policy_staged_jobs`. PostgreSQL
transactions don't span connections, and certainly don't span database
engines. So the supported adapter list is the set of ActiveJob adapters
whose jobs table is a regular PG table on the same role:

- **GoodJob** — writes to `good_jobs` in the host app's database.
- **Solid Queue** — writes to `solid_queue_jobs`, optionally on a
  separate `:queue` role. Both must be on PostgreSQL and our staging
  tables must be configured to share the same role
  (`DispatchPolicy.config.database_role = :queue`).

Sidekiq, Resque, SQS, and any other adapter whose backing store is
external (Redis, AMQP, an HTTP API) cannot participate in the
PostgreSQL transaction. There is no way to make their `enqueue` call
roll back if the staging TX aborts, so we'd inherit the failure modes
this gem was written to eliminate.

## Sketch: how external-adapter support could be added later

If we ever decide to support Sidekiq et al., the constraint shifts from
"two-phase commit" (impossible without XA-style coordinators) to
"recoverable two-phase pattern with adapter idempotency". Outline:

1. **Two-step admission with explicit transitions.** Add an
   `enqueued_at` column to `dispatch_policy_staged_jobs`. The state
   machine becomes `pending → admitted → enqueued → completed` instead
   of the current implicit `pending → admitted → completed`.

2. **Inside the staging TX**: set `admitted_at = now`, increment
   counters, and persist a `pending_enqueue_at` timestamp. Commit.

3. **Outside the TX**: call `adapter.enqueue` using the existing
   `active_job_id` as an idempotency key. The adapter must dedupe by
   that key (Sidekiq Pro's `unique_for` or `sidekiq-unique-jobs` for
   OSS Sidekiq; native `RPOPLPUSH` patterns won't help). On success,
   `UPDATE staged_jobs SET enqueued_at = now`.

4. **Recovery sweep**: a separate sweep finds rows where
   `admitted_at IS NOT NULL AND enqueued_at IS NULL AND
   pending_enqueue_at < now() - threshold` and retries the
   `adapter.enqueue` call. The adapter's idempotency contract makes
   the retry safe.

5. **Reaper expansion**: the existing reaper would have to distinguish
   "stuck pending enqueue" (retry) from "worker crashed mid-perform"
   (release counters only). Today it does only the latter.

### Why we haven't done this

- It's significantly more complex than the current PG-only design, with
  a new column, new states, a second sweep, and a hard dependency on a
  third-party Sidekiq plugin for idempotency.
- It pushes the trickiest correctness problem (idempotent enqueue) onto
  the user/operator. Get it wrong and you get either lost jobs (no
  idempotency) or double execution (sloppy idempotency window).
- The audience for whom "Sidekiq + admission control" matters more than
  "drop in GoodJob/Solid Queue" is small. Most teams adopting a Rails 7+
  job system in 2025+ are already on or moving to a PG-backed adapter.

If this changes, file an issue and link this section.

## Sketch: how SQLite support could be added later

Solid Queue runs on SQLite (it's the Rails 8 default for small apps),
so in principle dispatch_policy could too. The blockers are smaller
than they first appear — most PG-isms have SQLite equivalents, and
the few that don't already have a fallback in our own code.

### What SQLite has

| dispatch_policy uses              | SQLite story                                                        |
|-----------------------------------|---------------------------------------------------------------------|
| `FOR UPDATE SKIP LOCKED`          | Not needed. SQLite serializes writes for the whole DB, so two ticks can't claim the same row. |
| Partial unique index `WHERE completed_at IS NULL` | Supported since 3.8. Same DDL works.                          |
| `ON CONFLICT DO UPDATE` (counter increment) | Supported. Same syntax.                                       |
| `RETURNING *`                     | Supported since 3.35.                                               |
| `jsonb` (`partitions`, `context`, `snapshot`) | `TEXT` + `JSON1` extension. Reads use `json_extract`; ActiveRecord's serialized attribute support hides most of this. |
| `CROSS JOIN LATERAL` (`fetch_round_robin_batch`) | Not supported, but `fetch_time_weighted_batch` already loops over partitions — the same loop works. |

### What would need to change

1. **Migration variants per adapter.** `db/migrate/…create_dispatch_policy_tables.rb` currently writes `t.jsonb` and partial unique indexes with PG-specific options. Detect `connection.adapter_name` and emit `t.json` (or `t.text` + a JSON serializer) for SQLite. The partial index DDL is identical.

2. **Drop the `LATERAL` query path on SQLite.** `fetch_round_robin_batch` (`lib/dispatch_policy/tick.rb`) currently has two implementations: a `CROSS JOIN LATERAL` query plus a Ruby loop in `fetch_time_weighted_batch`. On SQLite, route plain round-robin through the loop too. Performance is fine because SQLite is in-process — there is no network round-trip to amortize.

3. **Smaller `batch_size` defaults.** SQLite serializes every write transaction to the entire database, so the tick's TX (now wrapping admission + adapter enqueue) blocks every other writer of the host app while it runs. With `batch_size = 500` and one `INSERT` per admitted job that's a meaningful pause for the rest of the app. Bias the default down (e.g. 50) when the configured connection is SQLite, and document the tradeoff.

4. **WAL mode required.** Document that SQLite must be in WAL mode (the Rails 8 default) so readers don't block while the tick TX is open. `journal_mode=DELETE` would make the rest of the app stall during every tick.

5. **Query dispatch by adapter.** Introduce a thin `Tick::Driver` (or just an `if connection.adapter_name == "SQLite"` switch in the 4-5 PG-specific spots). Concretely:
    - `fetch_round_robin_batch` — LATERAL vs. loop
    - `fetch_plain_batch` — drop the `lock("FOR UPDATE SKIP LOCKED")` call (SQLite has no equivalent and doesn't need it)
    - `PartitionInflightCount.increment` — already uses `ON CONFLICT`, should work as-is
    - JSON column reads in the admin UI — use ActiveRecord serialized attributes so the call sites don't change

### Why we haven't done this

- The audience overlap between "needs admission control" and "uses
  SQLite in production" is small. Admission control is most valuable
  at multi-tenant throughput where you've already moved to Postgres.
- The single-writer-at-a-time constraint changes the operational story:
  the tick TX becomes a global write barrier for the whole app, which
  surprises users in a way the PG version doesn't. Documenting this
  honestly takes more effort than the code change itself.
- We don't want a half-supported "it kinda works" story. Either tests
  cover both adapters and the docs commit, or we stay PG-only and tell
  the truth.

Total effort estimate if it ever matters: ~1-2 days of code + a CI
matrix for both adapters. Not architecturally hard; mostly schema and
query plumbing. File an issue with a real use case to revive this.

## Scale: what numbers can we support?

Numbers from `bundle exec rake test:benchmark` on local PostgreSQL
13.19 (single-node, no tuning, defaults). batch_size=500,
jobs_per_partition=3. Useful as a rough orientation, NOT an SLA —
remote PG, container limits, or noisy neighbours move these by
multiples.

| Operation                       |    100 |   1,000 |  10,000 | 100,000 |
|---------------------------------|-------:|--------:|--------:|--------:|
| `fetch_time_weighted_batch`     |   22ms |   107ms |   340ms | 1,089ms |
| `fetch_round_robin_batch`       |   12ms |    95ms | 1,025ms |   785ms |
| `Tick.run` end-to-end           |  328ms |   440ms |   423ms |   489ms |
| `Tick.reap` (all rows expired)  |  2.8ms |    11ms |   182ms | 2,610ms |
| `StagedJob.stage_many!`         |   15ms |   133ms | 1,316ms | 20,650ms |

### What this says

- **`Tick.run` is flat**, regardless of how many partitions are
  pending. Capped by `batch_size`. Roughly 1,000-1,200 admissions/sec
  steady-state across all scales tested.
- **`Tick.reap` is bulk-batched**: two SQL statements regardless of
  expired-row count (one UPDATE … RETURNING on staged_jobs, one
  UPDATE … FROM (VALUES …) on counters). ~40,000-90,000 rows/sec.
- **Fetch operations dominate the tick cost beyond ~10k partitions**.
  Time-weighted scales sub-linearly thanks to the CTE+VALUES driver.
  Plain round-robin sits at ~800-1,000ms in the 10k-100k range — the
  CTE pattern gave us plan stability (no more 44x run-to-run variance
  the SELECT DISTINCT version had), but the planner still flips
  between strategies based on LIMIT-per-LATERAL value (open issue).
- **`stage_many!` is just `INSERT … VALUES` throughput**: ~7,000
  rows/sec on local PG. For sustained enqueue rates higher than that
  you'd need `COPY` or batch enqueue from a streaming source.

### Recommended scale tiers

| Partitions | Status      | Notes                                                               |
|-----------:|-------------|---------------------------------------------------------------------|
|     ≤ 10k  | Comfortable | All ops sub-second. Default tick cadence (1s) leaves headroom.       |
|  10k–100k  | Workable    | Round-robin / time-weighted fetch take 0.5-1s per tick. Bump `tick_sleep` to 2-5s, or the tick will spend most of its time fetching. Reap stays fast. |
|    > 100k  | Investigate | Numbers extrapolate. Adding a covering index on `(policy_name, admitted_at, round_robin_key, priority, staged_at)` or capping partitions per tick will likely be necessary. |

Beyond 100k partitions the gem hasn't been benchmarked. Operationally
we have one data point: `pulso.run` runs in production with low
thousands of partitions and the tick is consistently <100ms. If you
operate at hundreds of thousands of partitions and try this gem, file
an issue with your numbers — it'd be useful baseline.

### Re-running the benchmark

```sh
SIMPLECOV_DISABLED=1 bundle exec rake test:benchmark             # up to 10k
SIMPLECOV_DISABLED=1 MAX_PARTITIONS=100000 bundle exec rake test:benchmark
```

Output is column-aligned markdown to stdout; progress bars on stderr
erase themselves so `… > report.md` captures only the report.
