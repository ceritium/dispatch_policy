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
