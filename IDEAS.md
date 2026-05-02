# Ideas / deferred

Items detected during development but deliberately deferred. Each
block records the symptom, a possible fix, and why it didn't ship in
the change at hand.

## Aggressive sweeper for orphan partitions with pending=0

**Symptom observed.** After a test that generated many distinct
buckets (`high_concurrency` with 10 001 unique partitions), most of
them sat in the table with `pending_count = 0` and
`total_admitted > 0`. Partitions are GC'd by
`sweep_inactive_partitions!`, but only after
`partition_inactive_after = 24h` of inactivity. Meanwhile they
bloat listings, UI queries, and the partitions table.

**Possible improvements.**

- Lower the default TTL to something more aggressive (1h? 15m?). The
  only thing lost when an empty partition is deleted is its
  `gate_state` (token bucket), which rebuilds itself when the
  partition reappears.
- Add a "GC partitions" button in the UI that calls
  `Repository.sweep_inactive_partitions!(cutoff_seconds: 0)` for
  on-demand cleanup.
- On the dashboard, surface "X empty partitions candidate for GC"
  as a separate panel from "active w/ pending".

**Why deferred.** Changing the global TTL affects every deployment
and can surprise people (losing live gate_state for partitions that
are momentarily empty). Better thought of as: per-policy config +
explicit UI.

## Revisit the sweeper and heartbeat interval

**Symptom.** Three coupled numbers without strong justification in
code:

- `inflight_heartbeat_interval = 30s` — how often InflightTracker
  refreshes `heartbeat_at` while the job runs.
- `inflight_stale_after = 5 * 60` — when the sweeper treats an
  inflight row as a zombie and deletes it.
- `sweep_every_ticks = 50` — how often (in TickLoop iterations) the
  sweeper runs.

The relationships that should hold:

- `inflight_stale_after >> inflight_heartbeat_interval`. The current
  10× ratio (300 / 30) leaves room for a couple of heartbeats to
  fail without losing the row. Document that touching one requires
  touching the other.
- `sweep_every_ticks * idle_pause < inflight_stale_after`. If the
  sweep runs every 50 ticks and each tick can take
  `idle_pause = 0.5s` (when idle), the sweep runs every ~25s in the
  worst case. Fine for 5min. But if the operator bumps `idle_pause`
  to 5s under load, the sweep runs every 250s = 4min, near the
  stale cutoff. Validate.

**Possible improvements.**

- Validate at `DispatchPolicy.configure` time that the relationships
  hold (logger warning when unbalanced).
- Replace `sweep_every_ticks` with `sweep_every_seconds` (time, not
  iterations) — more stable against changes to `idle_pause`.
- Surface the three metrics in the UI ("last sweep at", "stale rows
  pending sweep", "heartbeats in last minute") so the operator can
  tell whether the values fit their load.

**Why deferred.** The short-term priority was fixing the heartbeat
bug where it stopped refreshing during the perform (the 5min
sweeper killed inflight rows of legitimately long-running jobs).
Once that's fixed, this is defaults tuning and monitoring.

## `gate :global_cap` — rate-limit shared across all partitions

**Symptom / what's missing.** Today we have `tick_admission_budget`
as a per-tick cap at the policy level: it caps total admissions per
tick, distributed via `fair_share` across the claimed partitions.
That works as a global rate limit when the TickLoop runs at a
predictable cadence (cap=50 with idle_pause=0.5s ≈ 100/sec total),
but it has three blind spots:

- **Sensitive to tick rate.** Faster ticks under load admit more
  per second than slow ticks, even with the same cap.
- **No persistent token bucket.** The cap resets every tick. An
  external system that cares about request-per-time intervals
  (e.g. a contractual rate ceiling on a paid API) sees burstier
  traffic than the configured rate.
- **Per-shard, not cross-shard.** With 4 shards × `tick_admission_budget = 50`
  the system admits up to 200/tick globally. A true global cap
  would be shared across shards.

**Possible design.**

A new gate type:

```ruby
dispatch_policy :foo do
  partition_by ->(c) { c[:tenant] }
  gate :global_cap, rate: 5000, per: 60   # 5000/min globally
  gate :throttle,   rate: 100,  per: 60   # plus per-tenant rate
end
```

State lives in a single shared row, not per-partition:

- Option A: a row in `dispatch_policy_partitions` with a sentinel
  key like `partition_key = "__global__"` keyed off
  `(policy_name, "__global__")`. Reuses the existing
  `gate_state` jsonb column and the partition lock semantics.
- Option B: a new table `dispatch_policy_global_buckets` keyed by
  `(policy_name, gate_name)`. Cleaner schema; one extra table.

Option A is the lighter touch — same UPSERT machinery, same
`SELECT … FOR UPDATE` semantics, no migration cost. The only quirk
is making `claim_partitions` skip the sentinel row so it doesn't
appear in the operator UI as a fake "partition".

**Tick integration.** The `:global_cap` gate runs FIRST in the
pipeline (before `:throttle` and `:concurrency`) so its budget caps
all subsequent gates. Each tick, the gate:

1. Reads the global bucket state from the sentinel row.
2. Refills based on elapsed time: `tokens = min(rate, tokens + Δt × refill_rate)`.
3. Returns `allowed = floor(tokens)` to the pipeline (capped by
   `admit_budget` from the previous gate).
4. After admission, persists the consumed tokens via UPSERT in the
   same TX as the per-partition admit. The single sentinel row
   becomes a hot lock — only one tick worker (or shard) writes to
   it per admission. Acceptable as long as the TX time stays low.

**Cross-shard contention.** This is the cost. Multiple shard
workers admitting simultaneously serialize on the global bucket
row's `FOR UPDATE`. Mitigations:

- Keep the global TX short (the bucket math is O(1), microseconds).
- Add a soft tier: the gate optimistically reads the bucket without
  locking, returns its allowed count, and only locks during the
  per-partition admit to consume. Risk: two ticks see the same
  tokens and over-admit by one batch each. Bound is `2 × admission_batch_size`,
  acceptable for soft caps. Document as the trade-off.

**Operator visibility.** The dashboard shows the live token count
of each `:global_cap` gate (read from the sentinel row's
`gate_state`) plus the recent admit rate as a percentage of the
configured `rate`. `OperatorHints` fires when admits ≥ 80% of the
cap.

**Why deferred.** `tick_admission_budget` covers the 95% case.
A real `:global_cap` gate is justified when (a) the host needs a
hard contractual rate, (b) tick durations are highly variable, or
(c) cross-shard sharing of a single cap matters. Until those needs
land in real workloads, the per-tick budget plus operator
monitoring is the simpler tool.

## Staging-time `dedupe_key`

**Symptom / what's missing.** Upstream had a `dedupe_key` DSL that
collapsed enqueues sharing an identity (e.g. "one summary email per
member per day", "one check per monitor while a previous one is
still pending"). v2 dropped it: every `perform_later` stages a row,
and dedupe is the host's responsibility — either via
`good_job_control_concurrency_with` (GoodJob only) or via the
schedule that enqueues (cron with a unique key, idempotent inner
perform guarded by a digest, etc.).

The two real cases observed in opstasks:

- `SendDailySummaryEmailJob` relied on `dedupe_key
  "member:#{id}:date:#{today}"`. Workaround: the daily cron has a
  unique cron_key per (member, day), so it cannot double-enqueue.
- `MonitorCheckV2Job` relied on `dedupe_key "monitor:#{id}"` to
  drop a second enqueue while the first was pending. Workaround:
  `check_digest` already gates the inner perform, so the duplicate
  is admitted and its perform is a no-op.

Both have natural workarounds, but the workaround leaks into the
host (cron config, perform-side guard) what was a one-line
declaration in the policy.

**Possible design.**

```ruby
dispatch_policy :send_daily_summary_email do
  partition_by ->(c) { c[:account_id] }
  dedupe_key   ->(args) { "member:#{args.first.id}:#{Date.current}" }
  gate :throttle, rate: 100, per: 1.minute
end
```

Schema: a nullable `dedupe_key` column on `dispatch_policy_staged_jobs`
plus a partial unique index `(policy_name, dedupe_key) WHERE
dedupe_key IS NOT NULL`. Stage path uses `INSERT … ON CONFLICT DO
NOTHING`, returning whether the row was inserted. The job's
`successfully_enqueued = true` regardless (the caller wanted
"happen", and an existing row satisfies that).

Open questions when designing this for real:

- Should `dedupe_key` also block when an inflight row exists with
  the same key? Probably yes (the upstream semantics) — a second
  index/check on `inflight_jobs` would handle it. Cost: another
  unique index on a hot table.
- Replacement vs ignore: when a duplicate arrives with a later
  `scheduled_at`, do we update or drop? Upstream dropped silently;
  some users may want "extend the deadline". Default to drop, add
  `dedupe_key replace: true` only if a real case appears.
- Interaction with `Bypass`: bypass should skip dedupe entirely
  (caller explicitly opted out of staging).

**Why deferred.** Two data points, both with clean workarounds, and
GoodJob users already have the feature at the adapter level.
Staging-time dedupe adds a column, a unique index on a write-heavy
table, and a small but real semantic decision (replace vs ignore,
inflight vs staged). If a third case lands that doesn't fit a cron
key or a digest guard, that's the trigger to design this for real
instead of speculating about edge cases now.
