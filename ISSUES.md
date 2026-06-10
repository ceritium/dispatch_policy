# Known issues — code audit 2026-06-10

Findings from a full-repo review (admission core, enqueue/tracking path,
gates/policy DSL, dashboard/engine). Each issue has an ID used by the
remediation plan at the bottom. Line numbers are as of `ae820fa`.

> **A second-pass review** (after the fixes below) surfaced and fixed a few
> more: the generated good_job tick-loop job dying after one run
> (`total_limit` vs `enqueue_limit`), `record_sample!` bypassing the
> multi-DB role, the admin UI not reflecting the policy-level pause flag,
> `Context` indifference only at the top level, the tick loop crashing on
> `sweep_every_ticks = 0` / negative pauses, pass-2 denies missing from
> metrics, and assorted UI/dummy polish (sign in `format_count`, negative
> durations, ILIKE escaping, Turbo listener leak, dynamic throttle `per`,
> blank-field 500s). All are in the CHANGELOG (Unreleased).

> **A third-pass review** (over the fix branch itself) found and fixed one
> more medium: **M9 — the engine controllers bypass `config.database_role`**.
> H1 wrapped every `Repository` method, but the five dashboard controllers
> query the AR models directly (~25 call sites: `Partition.all`,
> `StagedJob.find`, `InflightJob.count`, `PolicySetting.paused`, …), so
> under multi-DB every dashboard page hit the default writing role
> (`PG::UndefinedTable` → 500) and `pause`/`resume` wrote the partition
> `status` to the wrong DB while the flag went to the right one. Fixed with
> an `around_action` in the engine's `ApplicationController` wrapping the
> whole action (including view rendering) in `Repository.with_connection`.
> While there, `pause`/`resume` now write the policy flag and the partition
> statuses in **one transaction** — previously two autocommitted statements
> could diverge if the process died between them.

Verified clean (no findings): SQL injection (everything goes through bind
params or whitelists), XSS (the only `html_safe` calls are static gem
assets), CSRF on dashboard actions, keyset pagination correctness,
migration vs. generator template drift, and the CLAUDE.md invariants
(admission TX atomicity, throttle double-spend in pass-2, claim
anti-starvation).

---

## High

> **Status:** ALL issues (H1–H2, M1–M8, L1–L10) fixed on branch
> `fix/high-priority-audit-issues`, each with regression tests. M6 adds the
> `dispatch_policy_policy_settings` table — existing installs must create
> it (the gem ships a single migration; copy the new `create_table` block
> or run the updated migration/generator).

### H1 — `config.database_role` is only applied in 3 call sites; multi-DB is broken end-to-end

`Repository.with_connection` (the `connected_to(role:)` wrapper) is only
used by `tick.rb:208`, `tick.rb:324` and `manual_admission.rb:30`.
Everything else runs against the default writing role:

- `Repository.stage!` / `stage_many!` — the entire `perform_later` path
- `Repository.claim_partitions` (`tick.rb:34`)
- pipeline reads: `count_inflight`, adaptive seed/record
- `record_tick_sample!`, all `TickLoop.sweep!` statements
- all of `InflightTracker` (insert, delete, lookup, and the heartbeat
  thread at `inflight_tracker.rb:150` checks out from
  `ActiveRecord::Base.connection_pool` directly)

In the exact scenario the option exists for (solid_queue on a separate
DB, gem tables living there): staging hits the primary DB →
`UndefinedTable`, or — worse — if the tables exist in both DBs, jobs are
staged into the primary while the tick reads the queue DB → **silent job
loss**. The concurrency gate also counts inflight rows in a different DB
than the one the tracker writes to → systematic over-admission.

### H2 — Two gates of the same type in one policy corrupt each other's state

The token bucket is always persisted under `gate_state["throttle"]`
(`gates/throttle.rb:49,62,74` — the key is the gate *type* name, not a
per-instance id) and the DSL does not reject duplicates
(`policy_dsl.rb:34-37`). With the classic multi-window pattern:

```ruby
gate :throttle, rate: 10,  per: 60     # 10/min
gate :throttle, rate: 600, per: 3600   # 600/h
```

both gates read/write the same state; `Pipeline` merges the patches over
the same key, so only the last gate's bucket survives. On the next tick
the first gate clamps the surviving token count to its own capacity and
sees a **permanently full bucket**: the strict 10/min limit silently
becomes "10 per tick". The same collision applies to two
`:adaptive_concurrency` gates (shared stats row, different parameters).

---

## Medium — all fixed

### M1 — Bulk enqueue: silent job drop, premature `successfully_enqueued`, missing `bypass_retries`

`job_extension.rb` (BulkEnqueue path):

1. `next unless policy` inside the `filter_map` (line ~123) drops a job
   whose policy name is not registered — it is neither staged nor
   delegated to `super`. The single-enqueue path delegates to the
   adapter in that case.
2. `job.successfully_enqueued = true` is set *before* `stage_many!`
   runs; if the INSERT raises, callers that rescue and check
   `successfully_enqueued?` believe the jobs were enqueued.
3. The bulk path does not honor `bypass_retries` the way the single
   path does, so retried jobs in a `perform_all_later` batch get
   re-staged instead of bypassing admission.

### M2 — `ManualAdmission.force!` does not pre-insert inflight rows

`manual_admission.rb:41-43` claims + dispatches but skips the
`insert_inflight!` step the tick performs, breaking the invariant
"every admitted job creates a row in inflight_jobs". Draining 200 jobs
into a partition with `concurrency max: 10` leaves the gate seeing
`in_flight ≈ 0` until each job actually starts → over-admission window
proportional to the drained backlog. It also skips
`record_partition_admit!`, so fairness decay never sees manual admits.

### M3 — Pre-inserted inflight row is orphaned for 1h when the job dies before `around_perform`

`ActiveJob::DeserializationError` (deleted GlobalID — typically handled
with `discard_on`) fires during argument deserialization, *before*
callbacks run, so `InflightTracker.track`'s `ensure` never executes.
The tick's pre-inserted row keeps `heartbeat_at == admitted_at` and only
falls into the "queued" sweeper tier (`inflight_queued_stale_after`,
default 1h). With `concurrency max: 5`, five discarded jobs block the
partition for an hour.

### M4 — Throttle with `rate <= 0` / `nil` denies without `retry_after`: per-tick busy-loop + backoff clobber

`gates/throttle.rb:32` returns `Decision.deny(reason: "rate=0")` with no
`retry_after`. In `bulk_record_partition_denies!`
(`repository.rb:333-336`) a NULL `retry_after` sets
`next_eligible_at = NULL`, which (a) makes the partition eligible again
on the very next tick — it gets claimed (`SELECT FOR UPDATE`), evaluated
and bulk-updated forever, consuming a `partition_batch_size` slot per
tick — and (b) clobbers any pre-existing backoff. Aggravating:
`capacity_for` uses `Integer(value)`, so a legitimate fractional rate
(`rate: 0.5, per: 1`) truncates to 0 and falls into the same hole with
no error.

### M5 — `AdaptiveConcurrency` does not validate `ewma_alpha` / decrease factors

`gates/adaptive_concurrency.rb:41-47` validates `target_lag_ms`, `min`
and `initial_max` but not:

- `ewma_alpha: 0` → EWMA frozen at its seed (0), never exceeds
  `target_lag_ms`, `current_max` grows +1 per successful perform without
  bound — the gate is functionally disabled.
- `failure_decrease_factor` / `overload_decrease_factor > 1` →
  multiplicative *increase* under failure/overload
  (`repository.rb:791,794` applies `FLOOR(current_max * factor)` without
  assuming factor < 1): positive feedback loop, the opposite of AIMD.
- `ewma_alpha > 1` → `(1 - α)` negative, EWMA oscillates in sign.

### M6 — "Pause policy" does not apply to partitions created after the pause

`policies_controller.rb:85` does `update_all(status: "paused")` over
existing rows, but `upsert_partition!` (`repository.rb:131`) always
inserts new partitions as `'active'` (ON CONFLICT doesn't touch status —
correct for existing rows, wrong for new ones). A tenant's first
`perform_later` after the pause creates an active partition that the
next tick admits. The pause is per-existing-partition, not per-policy —
broken in exactly the incident scenario the button exists for
(downstream outage + incoming traffic).

### M7 — `partitions#admit` accepts an unbounded `count` (and 500s on non-numeric input)

`partitions_controller.rb:70`: `Integer(params[:count] || 1)` with no
upper bound goes straight into `ManualAdmission.force!(limit: count)` —
a single `DELETE … RETURNING` + dispatch of everything in **one TX**,
bypassing the batching/cap that `drain` implements precisely to avoid
request timeouts and giant transactions. `Integer("abc")` raises → 500.

### M8 — Forged pagination cursor causes a 500 on timestamp sorts

`cursor_pagination.rb:94-108`: `decode` accepts any `(String, Integer)`
pair, and for the `stale`/`recent` sorts the string is bound against a
timestamp column. A non-parseable string
(`?sort=stale&cursor=<base64 of ["zzz",1]>`) raises
`invalid input syntax for type timestamp` → 500. The comment in `decode`
explicitly promises hostile payloads can't "reach the WHERE clause and
raise a 500"; the implementation doesn't deliver that for timestamps.

---

## Low — all fixed

- **L1** — `stage_many!` doesn't chunk: > 8,191 rows exceeds PG's 65,535
  bind-param limit and fails the whole batch (`repository.rb:81-123`).
- **L2** — The comment on `bulk_record_partition_denies!` claims the
  `claim_partitions` row locks are still held during the tick, but the
  claim runs in autocommit — locks release at statement end. Real
  exclusion relies solely on "one tick loop per (policy, shard)"; the
  comment asserts a safety property that doesn't hold.
- **L3** — Each heartbeat (30s) checks out an extra pool connection per
  running job (`inflight_tracker.rb:150`); pools sized exactly to N
  workers get periodic `ConnectionTimeoutError` → missed heartbeats →
  long jobs swept as stale.
- **L4** — `insert_inflight!` happens outside the tracker's
  `begin/ensure` (`inflight_tracker.rb:42-52`): if `start_heartbeat`
  raises, a ghost row remains until retry/sweeper.
- **L5** — DSL accepts `tick_admission_budget 0` /
  `admission_batch_size 0` → silent full stop of the policy
  (`policy_dsl.rb:47-49,64-66`). Related: `concurrency full_backoff: -1`
  also unvalidated.
- **L6** — Policy drain can reach ~2× the 10,000 cap
  (`policies_controller.rb:97-108`; the `break` is evaluated before each
  partition, and a single partition can contribute up to 10,000).
- **L7** — Drain with future-scheduled jobs loops "Drained 0; N still
  pending — click again" forever (claim excludes future `scheduled_at`,
  `pending_count` includes them).
- **L8** — `@inflight` in `partitions#show`
  (`partitions_controller.rb:66`) is referenced by no view and is scoped
  to the whole policy instead of the partition.
- **L9** — "Recent staged jobs" sorts by `(scheduled_at, id)` while the
  claim admits by `priority DESC, scheduled_at NULLS FIRST, id` — the
  operator sees roughly the inverse of the real admission order.
- **L10** — `Registry` reads (`fetch`/`names`/`each`/`size`) skip the
  mutex that guards `register`/`clear` — harmless on MRI, racy on
  JRuby/TruffleRuby.

---

# Remediation plan (high + medium)

One branch/PR per phase, in this order. Every fix lands with a
regression test (unit where possible, integration under
`test/integration/` when it needs Postgres).

## Phase 1 — H1: make `database_role` cover every DB touchpoint

The structural advantage: all SQL already funnels through
`Repository.connection`. Plan:

1. Wrap the body of every public `Repository` method in
   `with_connection { ... }`. Nested `connected_to` with the same role
   is a no-op, so the existing three call sites stay as they are.
2. `InflightTracker`: route insert/delete/heartbeat/lookup through the
   existing `Repository` methods (they already exist:
   `insert_inflight!`, `delete_inflight!`, `heartbeat_inflight!`); the
   heartbeat thread wraps its loop body in `Repository.with_connection`
   so it checks out from the role's pool.
3. Keep `Forwarder.dispatch` inside the admission TX — `connected_to`
   wraps *around* the TX, so the shared-connection invariant holds as
   long as the adapter's tables live in the same DB (already documented).
4. Tests: unit test that stubs `ActiveRecord::Base.connected_to` and
   asserts it wraps stage!/claim/track when `config.database_role` is
   set, and is skipped when nil. Integration tests keep running with
   role unset (single-DB) to prove the nil path is untouched.

## Phase 2 — H2: reject duplicate gate types per policy

1. In `Policy#validate!`, raise `InvalidPolicy` when two gates share the
   same `name` ("duplicate :throttle gate — use separate policies for
   multi-window limits"). Validation over per-instance state keys: it's
   the smallest fix, and per-instance keys would silently change the
   meaning of existing persisted `gate_state`.
2. README note: multi-window rate limiting needs separate policies.
3. Test: unit test asserting the raise; existing single-gate policies
   unaffected.

## Phase 3 — M1: bulk enqueue correctness

1. Move the "policy not registered" check into the
   `partition`-into-`with_policy`/`without_policy` step so those jobs
   fall through to `super` (adapter), matching the single path.
2. Honor `bypass_retries` in the bulk path (same predicate as the
   single path) before staging.
3. Set `successfully_enqueued = true` only *after* `stage_many!`
   returns, iterating the staged jobs.
4. Tests: bulk with an unregistered policy name delegates to the
   adapter; `stage_many!` raising leaves `successfully_enqueued?`
   false; retried jobs in a bulk batch bypass staging.

## Phase 4 — M2 + M3: manual admission & orphaned inflight rows

M2:
1. In `ManualAdmission.force!`, replicate the tick's pre-INSERT into
   `inflight_jobs` (same TX as the claim + dispatch) and call
   `record_partition_admit!` so fairness decay sees manual admits.
2. Test: after `force!`, `count_inflight` reflects the admitted jobs
   and a concurrency gate denies accordingly on the next tick.

M3:
1. Subscribe to the `discard.active_job` ActiveSupport notification in
   the railtie; on discard, delete the inflight row by
   `active_job_id`. This covers `discard_on
   ActiveJob::DeserializationError` (and any discard) where
   `around_perform` never runs.
2. Document that `inflight_queued_stale_after` remains the backstop for
   adapters/paths that don't emit the notification.
3. Test: a job discarded via `discard_on DeserializationError` (deleted
   GlobalID) leaves no inflight row behind.

## Phase 5 — M4 + M5: gate input validation & deny backoff

M4:
1. `capacity_for`: use `Float(value)` (keep fractional rates working);
   deny only when `<= 0`.
2. The `rate<=0` deny gets a `retry_after` (reuse `@per` as the natural
   re-check horizon — the soonest the rate could matter again).
3. `bulk_record_partition_denies!`: when `retry_after` is NULL, preserve
   the existing `next_eligible_at` instead of clobbering it
   (`CASE WHEN v.retry_after IS NULL THEN p.next_eligible_at ELSE … END`).
4. Tests: rate=0 partition is not re-claimed every tick; fractional
   rate admits at the right long-run pace; a NULL-retry deny does not
   erase a pre-existing backoff (extends the existing jsonb-merge
   integration test).

M5:
1. Validate in the constructor: `0 < ewma_alpha <= 1`,
   `0 < failure_decrease_factor < 1`, `0 < overload_decrease_factor < 1`.
2. Tests: each out-of-range value raises `ArgumentError` at
   policy-definition time.

## Phase 6 — M6 + M7 + M8: dashboard operator actions

M6 (policy-level pause — needs schema):
1. New table `dispatch_policy_policy_settings`
   (`policy_name` PK, `paused` boolean, timestamps). Per CLAUDE.md:
   edit the single migration *and* the generator template, add the
   columns to `repository_test.rb#schema_present?`, and ALTER TABLE
   manually in the live dummy app.
2. `pause`/`resume` upsert that row (keep the partition `update_all` so
   the partitions index still shows status).
3. `claim_partitions` adds
   `AND NOT EXISTS (SELECT 1 FROM …_policy_settings s WHERE s.policy_name = $1 AND s.paused)`
   — one indexed lookup per claim, no per-partition cost.
4. Test: pause policy → enqueue for a brand-new partition key → tick
   admits nothing; resume → it drains.

M7:
1. `Integer(params[:count], exception: false) || 1`, then
   `clamp(1, DRAIN_MAX_PER_REQUEST)`.
2. Test: controller test for `count=10_000_000` (capped) and
   `count=abc` (falls back to 1, no 500).

M8:
1. In `CursorPagination.decode`, when the sort column is a timestamp,
   require the cursor value to parse (`Time.iso8601` → rescue → treat
   the whole cursor as nil, first page).
2. Test: forged cursor on `sort=stale` returns 200 / first page.

## Out of scope here (tracked above as Low)

L1–L10 are deliberate deferrals: real but with bounded blast radius.
Candidates to batch into a cleanup PR after the phases above.
