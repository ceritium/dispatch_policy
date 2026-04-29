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
jobs_per_partition=3, table stats kept fresh with `ANALYZE` between
seed and measurement. Useful as a rough orientation, NOT an SLA —
remote PG, container limits, or noisy neighbours move these by
multiples.

| Operation                       |    100 |   1,000 |  10,000 | 100,000 |
|---------------------------------|-------:|--------:|--------:|--------:|
| `fetch_time_weighted_batch`     |   19ms |    16ms |    16ms |    71ms |
| `fetch_round_robin_batch`       |   11ms |    12ms |    13ms |    27ms |
| `Tick.run` end-to-end           |   37ms |    33ms |    35ms |    37ms |
| `Tick.reap` (all rows expired)  |  1.9ms |    14ms |   183ms | 2,395ms |
| `StagedJob.stage_many!`         |   15ms |   175ms | 1,488ms | 18,234ms |

### What this says

- **`Tick.run` is flat AND bulk**: ~50-65ms regardless of partition
  count, with 6 SQL statements regardless of `batch_size`. The
  per-row UPDATE for `mark_admitted!` and the per-row counter
  increment have both been folded into single statements
  (UPDATE … FROM (VALUES …) for staged_jobs, multi-row INSERT … ON
  CONFLICT for partition counts). ~7,500-9,500 admissions/sec
  steady-state on local PG; on remote PG with ~1ms RTT the impact
  is much larger because the old per-row pattern paid one round-trip
  per row.
- **`Tick.reap` is bulk-batched**: two SQL statements regardless of
  expired-row count (one UPDATE … RETURNING on staged_jobs, one
  UPDATE … FROM (VALUES …) on counters). ~30,000-70,000 rows/sec.
- **Fetch operations are the dominant per-tick cost at high partition
  counts**, but stay sub-second below 100k. Both use a CTE+VALUES
  driver so plans are deterministic and the SQL count is constant
  (2-3 queries) regardless of partition count.
- **`stage_many!` is just `INSERT … VALUES` throughput**: ~7,000
  rows/sec on local PG. For sustained enqueue rates higher than that
  you'd need `COPY` or batch enqueue from a streaming source.

### Recommended scale tiers

| Partitions | Status      | Notes                                                               |
|-----------:|-------------|---------------------------------------------------------------------|
|     ≤ 10k  | Comfortable | All ops well under 200ms. Default tick cadence (1s) is plenty.       |
|  10k–100k  | Workable    | Fetch grows to ~0.8-1.2s per tick at the upper end. Bump `tick_sleep` to 2-3s so the tick isn't back-to-back. Reap and Tick.run stay fast. |
|    > 100k  | Investigate | Numbers haven't been validated. Cap partitions per tick or add a more selective index if your query patterns differ.                       |

Beyond 100k partitions the gem hasn't been benchmarked. Operationally
we have one data point: `pulso.run` runs in production with low
thousands of partitions and the tick is consistently <100ms. If you
operate at hundreds of thousands of partitions and try this gem, file
an issue with your numbers — it'd be useful baseline.

### Operational note: keep table stats fresh

The fetch path picks between two indexes
(`idx_dp_staged_dispatch_order` on `(policy_name, priority,
staged_at)` and `idx_dp_staged_round_robin` on `(policy_name,
round_robin_key, priority, staged_at)`) based on Postgres' planner
estimates. With stale stats — typically right after a burst of bulk
inserts that outpaces autovacuum's analyze threshold — PG can pick
the worse index, and fetch wall-time inflates 10-50× compared to the
numbers above.

For high-throughput staging tables, lower the autovacuum analyze
threshold so stats keep up with the insert rate:

```sql
ALTER TABLE dispatch_policy_staged_jobs SET (
  autovacuum_analyze_scale_factor = 0.01,  -- 1% of the table changed
  autovacuum_analyze_threshold    = 100    -- min 100 rows changed
);
```

If you're hitting bursts so large that even tuned autovacuum lags,
schedule an explicit `ANALYZE dispatch_policy_staged_jobs` from cron
or a periodic job (it's cheap — ~100ms on 30k rows, ~500ms on 1M).
Symptom of staleness: a tick that suddenly takes 10× longer than
usual without a corresponding load change.

## Observability

Two complementary surfaces:

### `ActiveSupport::Notifications` events

Subscribe from anywhere — typically a Rails initializer that forwards
to your metrics sink (StatsD, Prometheus, Datadog, OpenTelemetry).

| Event                    | Payload keys                                                        |
|--------------------------|---------------------------------------------------------------------|
| `tick.dispatch_policy`   | `policy_name`, `admitted`, `partitions`, `declined` (true on rollback) |
| `reap.dispatch_policy`   | `reaped` (count of expired leases swept)                             |

`ActiveSupport::Notifications.instrument` automatically attaches
duration to each event, so dashboards can graph p50/p95/p99 of tick
wall time without extra work.

```ruby
# config/initializers/dispatch_policy_metrics.rb
ActiveSupport::Notifications.subscribe("tick.dispatch_policy") do |*, payload|
  StatsD.timing("dispatch_policy.tick_ms", payload[:duration])
  StatsD.gauge("dispatch_policy.admitted", payload[:admitted])
  StatsD.gauge("dispatch_policy.partitions_seen", payload[:partitions])
end
```

### `DispatchPolicy::Stats` snapshot

For health endpoints, console inspection, or scrape-style scrapers:

```ruby
DispatchPolicy::Stats.policy_summary("send_webhook_job")
# => {
#   policy_name: "send_webhook_job",
#   pending: 1234,
#   admitted: 12,
#   completed_24h: 88_213,
#   active_partitions: 540,
#   drained_partitions: 19_460,
#   oldest_pending_age_seconds: 0.83,
#   stale_partitions_60s: 3,
#   stale_partitions_300s: 0
# }

DispatchPolicy::Stats.summary  # array of one Hash per registered policy

DispatchPolicy::Stats.health   # :ok | :starvation | :backlog_aging | :inflight_drift
```

`stale_partitions_Ns` counts active partitions whose `last_admitted_at`
is older than N seconds. Spikes here indicate the round-robin cursor
is falling behind — typical knob to tune is `quantum` (lower = faster
rotation) or shard the policy across multiple TickLoops.

`oldest_pending_age_seconds` is the simplest single-number SLO. If
this exceeds your tolerance (e.g. monitor checks need < 60s freshness),
you've outrun the current capacity.

`Stats.health` returns a coarse signal suitable for `/up`-style health
probes. `:starvation` fires when any active partition has gone longer
than `lease_duration × 3` without admission; tune
`stale_threshold_seconds:` if your SLA is tighter.

### Tuning playbook: from metric to knob

Each row maps a symptom you'd notice on a dashboard to the Stats
field that confirms it and the config knobs that address it. Knobs
are listed in order of "try first" → "last resort".

| Symptom                                 | Confirming field(s)                                           | Knobs to try                                                                 |
|-----------------------------------------|---------------------------------------------------------------|------------------------------------------------------------------------------|
| Backlog growing, jobs taking too long   | `oldest_pending_age_seconds` rising; `pending` rising         | `batch_size` ↑ → admit more per tick. `tick_sleep_busy` ↓ → tick faster. Then add a parallel TickLoop scoped to a different policy/queue. |
| Specific tenants seem starved           | `stale_partitions_60s` > 0; same tenant id consistently       | `round_robin_quantum` ↓ (toward 1) → more partitions per tick. Verify policy is round-robin (`round_robin_by`) — plain policies don't rotate. |
| Tick wall time spiking                  | `tick.dispatch_policy` p95 of `duration`                      | `round_robin_max_partitions_per_tick` ↓ → bound the LATERAL plan. Drop to e.g. `batch_size / 4`. Check stats freshness (`ANALYZE` cadence) — stale stats can flip the planner to a 10× slower path. |
| Adapter rejecting writes                | `tick.dispatch_policy` payload `declined: true`               | Inspect adapter health (GoodJob `good_jobs` table writable? Solid Queue `:queue` connection healthy?). The Tick TX rolls back, so no data loss — just retry next tick. If chronic, your adapter DB is the bottleneck. |
| Workers crashing mid-perform            | `reap.dispatch_policy` `reaped` count climbing                | Investigate worker logs. `lease_duration` should be just above your worst perform — too short forces premature reap, too long delays counter recovery. Default 2 min suits most. |
| Counters drifting high (admit blocked)  | `admitted` plateauing while `pending` rises                   | Run `Tick.reap` manually if not already on a TickLoop schedule. Lower `lease_duration` if leases routinely outlive perform. |
| `partition_states` table bloating       | many `drained_partitions` per `active_partitions`             | `partition_drained_purge_threshold` ↓ (e.g. 0.3) → trigger ratio purge sooner. Or shorten `partition_idle_ttl`. |
| Single tick hot-loops the same tenant   | `partitions` in `tick` event ≪ `batch_size`                   | A burst tenant: top-up was the old culprit; current refill_round_robin stays inside the cap'd LRU subset. Verify by sampling `last_admitted_at` distribution per partition — uniform-ish = healthy. |
| `:starvation` health alert              | `Stats.health == :starvation`                                 | Check `stale_partitions_300s` for the count. Same fix as starvation row above (lower quantum, shard). Adjust `stale_threshold_seconds` if your SLA is < `lease_duration × 3`. |
| `:backlog_aging` health alert           | `Stats.health == :backlog_aging`                              | At least one pending row > 5min old. `batch_size` ↑ or scale workers; if a single policy dominates, shard it into its own TickLoop. |

### Single-number SLOs

`Stats.slo(policy_name, **budgets)` returns the four canonical
signals with the operator's budgets attached and a verdict where
applicable:

```ruby
DispatchPolicy::Stats.slo("monitor_check_v2_job",
  latency_budget_seconds:     60,
  fairness_threshold_seconds: 60,
  throughput_window_seconds:  60)
# => {
#   policy_name: "monitor_check_v2_job",
#   latency:    { seconds: 0.83, budget: 60, ok: true },
#   fairness:   { stale_partitions: 0, threshold: 60, ok: true },
#   throughput: { admissions_per_sec: 41.2, window_seconds: 60 },
#   capacity:   { active_partitions: 540, batch_size: 500,
#                 utilization: 1.08, headroom: false }
# }
```

`Stats.slos(**budgets)` returns one Hash per registered policy,
suitable for a JSON endpoint or a periodic scrape.

What each signal means:

- **Latency** — `oldest_pending_age_seconds` against your freshness
  budget. The single most important SLO. Alert when `ok: false`.
- **Fairness** — count of active partitions whose last admission
  (or first stage, for never-admitted ones) is older than the
  threshold. `ok: true` ⇔ no rotational starvation.
- **Throughput** — admissions per second over a rolling window. No
  `ok:` flag because only you know your ingress rate; alert when
  `admissions_per_sec < your enqueue_rate × safety_factor`.
- **Capacity** — `active_partitions / batch_size`. `headroom: true`
  when batch can hold every active partition this tick (no rotation
  needed). When `false`, you're rotating and the fairness SLO
  becomes the operative constraint.

### Worked example: tuning for monitor checks

Setup: 50,000 monitors, each enqueues 1 check/min. Target latency
< 60s freshness for any monitor.

Initial config (defaults): `batch_size=500, quantum=50, busy_sleep=0.05s`.

After deploy, dashboard shows:
- `oldest_pending_age_seconds`: 4 minutes (FAIL — over budget).
- `stale_partitions_60s`: 12,000 (most monitors not seeing rotation).
- `tick.dispatch_policy.duration` p95: 80ms.
- `pending`: 1,200, growing slowly.

Diagnosis: rotation is the bottleneck. With `batch_size/quantum = 10`
partitions per tick, round-trip on 50k monitors takes ~5,000 ticks =
5,000 × 80ms ≈ 7 minutes. Way over the 60s target.

Tuning sequence:

1. Drop `quantum` to 1: `partitions/tick = 500`, round-trip = 100
   ticks ≈ 8s. Re-measure: `oldest_pending_age_seconds` should fall
   under 60s.
2. If `tick.duration` rises (more partitions in pluck means more
   partition_states writes): set
   `round_robin_max_partitions_per_tick = 500` to keep the cap tight
   to batch_size.
3. If still tight, add `tick_sleep_busy = 0.01`: 4× tick rate when
   busy. Round-trip drops to 2s. Watch CPU on the worker.
4. If hardware-bound at this point, shard the cron: instead of one
   `DispatchTickLoopJob`, run 4 with disjoint policy_name args.

Each step is reversible from a config initializer, no migration.

### Re-running the benchmark

```sh
SIMPLECOV_DISABLED=1 bundle exec rake test:benchmark             # up to 10k
SIMPLECOV_DISABLED=1 MAX_PARTITIONS=100000 bundle exec rake test:benchmark
SIMPLECOV_DISABLED=1 ANALYZE_AFTER_SEED=0 bundle exec rake test:benchmark  # see worst-case stale-stats behaviour
```

Output is column-aligned markdown to stdout; progress bars on stderr
erase themselves so `… > report.md` captures only the report. To dig
into where time is spent inside the fetch, run
`bundle exec rake test:profile` — it captures every SQL the fetch
path fires and prints `EXPLAIN (ANALYZE, BUFFERS)` for each.
