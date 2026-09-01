# Known issues — code audits

Full-repo reviews, newest first. Each finding carries an ID its
remediation plan refers to; IDs are unique across audits. Line numbers
are as of the commit named in the audit heading.

---

# Follow-up pass 2026-09-02 — A1's lock order on the pause button, and A8/A10/A11/A12

The fourth audit fixed A1 where it was measured — the deny flush — and
CLAUDE.md recorded, as a known gap, that two other multi-row writers of
`dispatch_policy_partitions` still had no lock order. One of them is the
pause button. That one is fixed here, together with the four low findings
whose common shape turned out to be the same as A10's and A11's: **a
comparison whose two sides come from different clocks.**

## Review round on this branch — three of the six fixes were defective

Same discipline as the fourth audit's fix branch, same result, which is
why it is recorded rather than quietly corrected. Three independent
adversarial reviewers, each required to reproduce every claim by running
something. What they found was again **in the fixes, not in the audit**:

- **The pause fix introduced a worse bug than the one it fixed.** Dropping
  the transaction made the two writes non-atomic against a CONCURRENT
  click, not just against a crash. Resume clears the flag while pause is
  still walking its slices, pause's remaining slices land afterwards, and
  the policy ends with `paused = false` and every partition
  `status = 'paused'`: nothing admits, the dashboard says the policy is
  running, and it never heals — `upsert_partition!` does not write
  `status`, and the sweeper needs a `pending_count` of 0 that an
  unclaimable partition never reaches. 5 corrupt runs in 6 with the clicks
  2ms apart; master's single transaction: 0 in 6. Fixed with a
  `pg_try_advisory_lock` per policy, which serializes the clicks without
  putting a row lock near the enqueue path.
- **The A12 fix did not fix A12 at the sizing A12 names.** Demand went
  from `2N` to 1, but at `pool == threads` the supply is 0 either way:
  with pool=3 and 3 running jobs every beat still raised
  `ConnectionTimeoutError` and the gem's own sweeper deleted all three
  inflight rows while the jobs ran on. The fix is real but conditional on
  `pool >= threads + 1`, which is now stated as a requirement in
  `config.rb`, the CHANGELOG and CLAUDE.md, and the one timeout that says
  so is logged at error level instead of as an indistinguishable warning.
- **Two of the new tests were decorative**, and the mutation battery found
  one of them only in a FULL run: counting the heartbeat's statements
  races the loop, so mutation 44 was CAUGHT alone and SURVIVED among 43.
  The other was an upper-bound assertion on the adaptive lag, which the
  failure mode itself satisfies — an unknown wait is recorded as 0, and 0
  is less than any bound. Both replaced with assertions that have no
  window: the contents of the first beat, and a two-sided range.

Three further defects the reviewers reproduced and this branch also fixes:
the partition sweeper's DELETE was the last unordered multi-row writer of
`partitions` (4-10 deadlocks per 20s run, and in one of them the
operator's click was the victim); `StagedJob.due` — the scope the drain
button counts with — was still on the session clock; and a forked child
inherited the heartbeat registry and beat its parent's jobs, keeping rows
fresh that nothing would ever release.

Two gaps they found in the tests rather than the code are closed with
mutations 45-51, and one they found in the reasoning is recorded above:
`clock_timestamp()` vs `now()` was unpinned by any test even after the
range fix, because the range closed the "measured after the block" half
and not the "transaction timestamp" half.

## P1 — Pausing a policy deadlocks against an ordinary bulk enqueue

`PoliciesController#pause` / `#resume` wrote every partition row of the
policy with `Partition.for_policy(name).update_all(...)`, inside a
transaction, with no lock order: a seq scan locks in heap order, an index
scan in the database's collation, and neither is the byte order
`stage_many!` sorts by. Reproduced deterministically (two connections, the
enqueue side holding the byte-lowest key and reaching for the highest only
once the click is provably blocked on a row lock):

```
MODE=old  errors: [["click", "ActiveRecord::Deadlocked", …]]
          policy flag paused: nil     partitions marked paused: 0/2
MODE=new  errors: []
          policy flag paused: true    partitions marked paused: 2/2
```

That is the whole severity: the deadlock rolls back the controller's
transaction, so the policy is **not** paused, the tick keeps admitting,
and the request answers 500 with nothing saying the pause failed —
during the load that made someone click pause, which is the only time
anyone does.

Fixed with an ordered `SELECT … FOR UPDATE` before the write, exactly as
A1 was, and sliced (`PARTITION_STATUS_BATCH`) so a policy with tens of
thousands of partitions does not hold every row lock behind one click —
which would replace A1's deadlock with A1's lock convoy. Slicing costs
all-or-nothing, so the controller orders its two writes to fail closed
instead: the policy-level flag (the source of truth the tick reads) is
written first on pause and last on resume, and every partial state is
"more paused", never "the UI says paused while admission runs".

## A11 — Scheduled work read on the database session's clock

Every datetime column here is `timestamp WITHOUT time zone`.
`scheduled_at`, and the `scheduled_eligible_at` horizon derived from it,
are written by the application and serialized by `quoted_date`; the four
comparisons that decided whether that work was due used `now()`, a
timestamptz, so Postgres reinterpreted the stored value in the session
TimeZone:

```
SET timezone='America/New_York';
SELECT timestamp '2026-09-01 22:25:58' <= now();   -- a job due in 1 minute
 ?column?
----------
 f                                     -- and for the next four hours
```

Rails sets that session to UTC by default, which is why this hid: it
needs a host that sets `variables: { timezone: … }` in `database.yml` —
a supported knob, commonly used to make raw psql output readable. Then
`set(wait:)` runs early on a zone east of UTC and not at all on one west
of it, and nothing anywhere records the difference. Fixed by binding
`config.now` on both sides. `next_eligible_at` deliberately stays on
`now()`: Postgres writes it, so that is the clock it must be read on.

## A10 — The adaptive gate's feedback signal spanned two clocks

`queue_lag = Time.current - admitted_at` subtracted the worker's clock
from a timestamp Postgres wrote on the tick's connection. It is the AIMD
controller's only input, so a host a few hundred milliseconds fast against
`target_lag_ms` reads every job as late, shrinks `current_max` on every
observation and never grows it back — the cap collapses to `min` and stays
there. The lag is now computed by the database in the same statement that
already reads the admission row, with `clock_timestamp()` rather than
`now()` so a host that wraps the perform in a transaction does not report
"time since that transaction opened".

## A12 — The heartbeat competed with the workers for their own pool

One thread per running job, each checking out its own connection, against
a pool the Rails default sizes to the worker's thread count — while every
performing job holds one for the length of its perform. Demand was
therefore `2N` against a pool of `N`: the beats queue behind
`checkout_timeout`, raise, get swallowed as best-effort, and
`heartbeat_at` stops advancing. That is exactly what the stale sweeper
reaps, so a still-running job loses its inflight row and the concurrency
gate re-admits against an occupied slot — the cap lapses under precisely
the load that makes it matter.

Now one process-wide thread beats every running job in one statement, so
the heartbeat's connection demand is a constant 1 instead of `N`, which
fits the single spare connection the adapters already ask for. It does
not make starvation impossible; it stops it scaling with concurrency. The
beat's `RETURNING` also tells the registry which rows still exist, so an
id whose thread was killed before `track`'s ensure is dropped rather than
carried for the life of the process.

## A8 — A scheduled workload read as a stuck tick

Partitions parked behind a future horizon counted as "never checked" —
which fires the hint "the tick is not getting through them — increase
partition_batch_size or shard", pointing the operator at the one knob
that cannot help — and their frozen `last_checked_at` dragged the
round-trip percentiles toward infinity. They are excluded from both and
reported on their own as `schedule_parked`.

## A9 — left open, deliberately

The pending sparkline drops periods with no tick samples, so a tick loop
that dies reads as a backlog that stopped growing. The query change is
easy and would be wrong: zero-filling the gaps asserts a pending total of
zero for a window nobody measured, which is a different lie in the same
place. The honest fix is a gap-aware series plus an explicit "no ticks in
this window" signal — a UI change with its own design, not a tweak to
`tick_samples_buckets`, and out of scope for a pass about clocks and lock
order.

---

# Audit 2026-08-30 — whole-gem hunt, fourth pass (`c4c6475`)

Six hunters over the ground the first three passes did not cover, each
finding attacked by an independent verifier that accepted it only after
reproducing the failure itself. Twelve defects confirmed; the seven at
medium and above are fixed here, the five low ones are listed below for
a later pass.

The shape is worth recording, because it is what three passes of
subsystem-by-subsystem auditing structurally could not see: **the core is
sound and the edges are not.** Four tick loops racing one shard produced
1000 distinct jobs at the adapter with 0 duplicates and 0 losses; a 25s
chaos run of 23,850 enqueues against aggressive sweeps and 1,998 drains
left no orphaned rows and no `pending_count` drift; a backend killed
mid-admission rolled back cleanly. What broke was change (editing
`partition_by` or `shard_by` on a live install), deployment (the
documented multi-database install), interaction (two ordinary
participants deadlocking), and failure (one bad row wedging a partition
forever).

> **Status:** A1-A7 fixed. A8, A10, A11 and A12 fixed in the follow-up
> pass below; A9 still open, with its reasoning there.

## Five review rounds on the fix branch

The fixes themselves were reviewed five times, and the first three each
found them defective — worth recording, because the pattern is specific:
every defect was in a *fix*, none in the original audit. (Numbered
Round N, not RN — the R IDs are taken by the 2026-08-13 review.)

- **Round 1** — four of seven fixes broken; two did not work at all. A bare
  `ORDER BY` inherits the database collation, so the deadlock the
  `COLLATE "C"` sort exists to prevent survived it (18 in 20s); and
  narrowing `Repository`'s role routing to `with_connection` dropped the
  engine's own models out of it.
- **Round 2** — a test that pinned the bind order instead of the SQL stayed
  green with the `ORDER BY` deleted; another installed its own
  subscription, so the railtie it claimed to cover was invisible to it;
  a third turned red when the *correct* fix was applied.
- **Round 3** — the operator hint added for held-back rows pushed a bare Hash
  where the template calls `hint.level` / `hint.message`, so
  `GET /dispatch_policy` 500'd whenever anything was held: the page the
  same commit added a tile to, failing in exactly the state the hint
  exists to surface. No request-level test existed to see it.
- **Round 4** — nothing shipped broken. Two gaps: the `deserialize!` rescue
  stopped at `NameError`, so anything else out of `klass.deserialize` —
  a `KeyError` from an override reading a field a pre-upgrade payload
  lacks, a `TypeError` from a `scheduled_at` this gem did not write —
  still wedged a partition permanently; and CLAUDE.md still instructed
  the next maintainer, as a prohibition, to re-narrow the rescue that had
  just been widened. Both fixed.
- **Round 5** — nothing broken again, and every new test verified red against
  its parent commit. But the justification written for the widest rescue
  in the gem did not survive being checked: it cited a Float
  `scheduled_at` as what "Rails <= 7.1 wrote", and ActiveJob has written
  iso8601 since 7.1 — the gemspec floor. The rule was right and the wedge
  reproducible; the reason was checkable and wrong, in the one file whose
  job is to stop the next narrowing. Corrected to name the mechanism
  instead of enumerating classes.

The recurring lesson is one thing: **a test must fail against the code as
it was before the fix.** Four of the five defective tests above passed
against the bug they were written for. The mutation battery exists
because that check cannot be made by reading, and round 5 made it
routine: every production file a fix touches gets reverted to its parent
in a scratch copy, and the new test has to go red.

## High

### A1 — The deny flush deadlocks against bulk enqueues

`bulk_record_partition_denies!` is one statement, so it locks in heap
order while `stage_many!` deliberately sorts. One tick loop plus one
`perform_all_later` process: 16 deadlocks in 20s. Half aborted the
caller's batch mid-flight, half killed `flush_denies!` — which only logs
— so every denied partition in that tick lost its backoff and its
gate_state patch. Fixed with an ordered `FOR UPDATE` before the UPDATE.

### A2 — One undeliverable staged row wedges its partition forever

A `job_class` that no longer resolves rolls the batch back (correctly),
but the claim orders it to the head of the partition every time, so the
healthy rows behind it are never admitted again. No exit existed in the
product. Fixed by quarantining the row (`failed_at`), retrying the
admission once, and surfacing it in the UI.

### A3 — Introducing or changing `shard_by` strands every existing partition

The shard was pinned on first write and never rewritten, so partitions
that predate the change keep a shard no loop is started for. New
partitions drain normally, so the dashboard looks healthy. Fixed by
recomputing the shard while the partition is drained.

### A4 — `config.database_role` flips the role process-wide

`ActiveRecord::Base.connected_to` swaps the role for the whole hierarchy
and still leaves an adapter that writes through its own record class on
another connection — so the documented separate-queue-database install
could not admit a job, and its atomicity guarantee never held. Fixed
with `config.database_connection_class`, verified against two real
databases.

## Medium

### A5 — Editing `partition_by` silently removes the concurrency cap

Both concurrency gates counted in-flight rows under a key recomputed from
ctx, while the admission path files everything under the partition row's
key. Fixed by reading the row. CLAUDE.md's "by construction the same
value" is what hid this for three audits.

### A6 — A jsonb-retyped cap wedges the partition

`Integer()` on a `max:` that came back from jsonb as `"5.0"` raises
inside the admission TX. Fixed with `Float().floor`.

### A7 — The staged claim orders by an unindexed column

`priority` is in no index, so a deep single-partition backlog sorts
itself on every admission, twice per tick: measured at 500k rows,
~291 ms and 13.7 MB of temp files per claim, against 0.038 ms with
`idx_dp_staged_claim_order`. The index costs +0.08 ms per enqueue and
25 MB at that size.

A note here briefly retracted the temp-file figure as unreproducible.
That retraction was wrong and is withdrawn: it was measured with the
`FOR UPDATE SKIP LOCKED` stripped off, and nothing issues the statement
that way. `LockRows` sits between `Limit` and `Sort`, so the limit is
never pushed into the sort and it spills at ANY limit — 13.7 MB even at
`LIMIT 1`. Remove the locking clause and the same query becomes a 40 kB
top-N heapsort, which is where the "could not reproduce" came from.

## Low — recorded

All but A9 are fixed; see the follow-up pass at the top of this file.

- **A8** *(fixed)* Round-trip stats count schedule-parked partitions as
  "never checked", so a normal `set(wait:)` workload shows a red dashboard.
- **A9** *(open)* The pending sparkline averages across policies and drops
  empty periods, so a dead tick loop reads as a falling backlog.
- **A10** *(fixed)* The adaptive gate's `queue_lag` subtracts the worker's
  clock from the database's, so host skew above `target_lag_ms` shrinks the
  cap permanently.
- **A11** *(fixed)* Scheduled-work comparisons mix an app-written timestamp
  with Postgres `now()`, so a non-UTC session TimeZone runs `set(wait:)`
  jobs early (or never).
- **A12** *(fixed)* The heartbeat thread contends for the same pool the
  worker holds for the whole perform; at the Rails-default pool size a beat
  that lands early and then starves can get a still-running job swept as
  stale.

## Verified against the real adapter

The previous three passes never ran good_job or solid_queue — the whole
atomicity contract rested on reading the adapter source. Closed here
against a live good_job install:

```
perform_later x5   -> staged=5, good_jobs=0
tick               -> admitted=3 (throttle rate: 3), staged=2, good_jobs=3
bucket after       -> 0
failure after the adapter enqueue, inside the admission TX
                   -> good_jobs 3 => 3, the 2 staged rows returned
```

The last line is the at-least-once guarantee itself: the adapter's INSERT
rolled back with the gem's transaction.

---

# Audit 2026-08-29 — throttle review + subsystem hunt (`749274a`)

Third full review. Two halves: a line-by-line review of the throttle
atomicity branch (PR #38, since closed and folded in here), and a hunt
across the subsystems the first two audits did not cover — the enqueue
path, the dispatch/inflight handoff, the non-throttle gates with
fairness and metrics, and the dashboard/config/generator surface.

Everything below was reproduced against real Postgres before being
fixed, and every fix carries a regression test verified by mutation:
breaking the production line turns the suite red. Baseline going in:
218 runs / 517 assertions. Coming out: 262 runs / 621 assertions.

> **Status:** every finding is fixed on
> `fix/throttle-atomicity-and-partition-lifecycle`. The narrative for
> each — scenario, measurement, and what the fix does not promise —
> is in CHANGELOG.md under Unreleased.
>
> A review of that branch itself then found six more, all fixed on it:
> H10's release aimed at the writing pool rather than the role's, so the
> leak survived on a multi-database install; H11's regression test was
> vacuous (the job died inside `track`'s ensure, and the reaping rule was
> unreachable in an initializer block — inverting it left the suite
> green); M13's park could still hide a job that became due between the
> claim and the park, and its comment still promised the guard the commit
> removed; H8's savepoint swallowed `ActiveRecord::Rollback`, which would
> have let an admission commit with nothing in the adapter; and M13's new
> column was left out of the tick-order index while its upgrade note
> prescribed a column type the migration does not produce.
>
> A second review pass then found one more, in M18's own consequences:
> a forced admission drives the bucket arbitrarily negative, and the
> resulting backoff overflowed the Postgres interval parser — which
> discarded the whole tick's deny flush, restoring M4's busy-loop for
> every partition of that policy. Fixed by multiplying an interval
> instead of parsing one from text.

## Blocker

### B1 — The generated tick-loop job dies on its first iteration under solid_queue

`adapter_shutting_down?` called `SolidQueue::Process.current_process`, a
method solid_queue has never had. It is the `stop_when` lambda, called
outside the rescue that guards `Tick.run`, so the NoMethodError escaped
`perform` and the self-re-enqueue chain died after one run — while
`perform_later` interception kept staging jobs nothing would admit.
Fixed by using the ActiveJob `stopping?` hook both adapters implement,
plus making a raising `stop_when` non-fatal.

## High

### H6 — The token bucket was a read-modify-write

Two tick loops on one `(policy, shard)` each evaluated a full bucket,
each admitted it, and the second write overwrote the first, so one
admission went uncharged and the effective rate became `rate x loops`
indefinitely. Settled inside the admission UPDATE now.

### H7 — The bucket was read on one clock and charged on another

The charge took its timestamps from Postgres `now()` while `evaluate`
refills from `config.now`. Any offset became free tokens on every
evaluate (measured: 100 jobs in ten ticks against `rate: 10, per: 60`),
and `now()` being the transaction timestamp froze it inside an
enclosing transaction. The gate's clock is bound as a parameter now, and
the stamp is monotonic.

### H8 — A job class deferring its own enqueue destroyed admission

`enqueue_after_transaction_commit = true` (Rails-recommended for apps
enqueuing inside transactions) reroutes the forward onto the gem's own
admission transaction: it lands after COMMIT, outside `Bypass`. The
scheduled path re-staged forever leaking an inflight row per tick; the
immediate path rolled the admission back forever. Neither reached the
adapter. Fixed with a non-joinable savepoint around the enqueue.

### H9 — The periodic sweeper never ran

`TickLoop.run` counted iterations in a local while the generated job
re-enters `run` every `tick_max_duration`, and the shipped defaults put
exactly `sweep_every_ticks` iterations in a window. Nothing was ever
swept; a stale inflight row wedged a concurrency partition permanently,
in a loop that feeds itself. The counter is module state now.

### H10 — The heartbeat thread leaked a connection per running job

`connection_pool.with_connection` does not release a lease the pool
treats as permanent, which is what a bare `Thread.new` gets. One
connection per tracked job was pinned for the job's life, so a worker
sized by the Rails default raised ConnectionTimeoutError. Released
explicitly now.

### H11 — An inflight row orphaned for an hour without `discard_on`

`discard.active_job` is emitted only by the handler `discard_on`
installs, so a job dying before `around_perform` without one (the
routine `ActiveJob::DeserializationError`) left its row until the 1h
sweeper — an hour of a frozen tenant under `gate :concurrency, max: 1`.
The railtie also subscribes to `perform.active_job` now. M3's fix
covered only the `discard_on` subset.

## Medium

### M13 — A job due now waited behind a future-scheduled sibling

M10 parked partitions in `next_eligible_at`, the gate backoff column, so
no enqueue could clear it without resurrecting the M4 busy-loop. Moved
to `scheduled_eligible_at`.

### M14 — The sweeper's retention answered the wrong question

"One window has passed" is neither necessary nor sufficient for "the
bucket has refilled": too long for a partly-spent bucket, too short for
one in debt or on a sub-unit rate. The early-collection clause added
alongside it compared the stored snapshot, which nothing refreshes while
a partition is idle, so it was inert. Both replaced by refilling the
stored bucket to now.

### M15 — The catch-all sweep reset the bucket of an unloaded policy

"Absent from this process's registry" is not "deleted from the code" —
R3 recorded the same trap in `ManualAdmission`. A row still carrying a
bucket waits out `config.unknown_policy_retention` now.

### M16 — Priority was applied backwards

The claim ordered `priority DESC` while both adapters mean a smaller
number is more urgent, so a host's urgent job was admitted last and
could starve. The dashboard mirrored the inversion.

### M17 — The adaptive stats table had no GC

`adaptive_seed!` runs on every evaluate and nothing ever deleted from
`dispatch_policy_adaptive_concurrency_stats`, so it grew with the
lifetime cardinality of `partition_by`. Swept now, anti-joined against
the partitions table.

### M18 — A forced admission escaped the throttle's cost

The UI admit/drain bypassed the gate's decision, which is the point, and
also its charge, which is not: a drain handed the tenant everything it
forwarded plus an untouched window.

### M19 — One poisoned staged row killed the whole drain

No per-partition isolation in the UI drain, so an undeserialisable row
raised out of the controller as a bare 500 and every healthy partition
behind it was never reached.

---

# Audit 2026-08-13 — v0.5.0 (`d1eb259`)

Second full-repo review, focused on admission correctness. Baseline
before the review: 188 runs / 451 assertions green against a local
Postgres. Everything marked **reproduced** below was verified with a
throwaway integration test against real Postgres, not by reading alone.

Verified clean (no findings): admit/dispatch atomicity and rollback,
the pass-2 throttle double-spend guard (the in-memory `gate_state`
mirror holds), the claim's anti-stagnation ordering, the parameter
indexing of the decay UPDATE, and `exec_query` type casting — jsonb
comes back as a String (hence `parse_jsonb`) while timestamps come back
as UTC `Time`, so the `Time.parse` fallbacks in `Forwarder` and
`InflightTracker` are dead code rather than a timezone bug. Also clean:
the `database_role` wrapper and cursor pagination.

## High

> **Status:** every finding in this audit (H3–H5, M10–M12, L11–L17) is
> fixed, each with a regression test verified to fail against the code it
> replaces. H3–H5 landed in #35; M10–M12 and the L cleanup followed.
>
> A review of the Phase 1 branch itself found that the first version of
> the fix only closed the wedge for classes declaring their policy with
> the `dispatch_policy` macro: creation was decided from the registered
> policy at tick time, release from the macro call site, so a class bound
> with `dispatch_policy_name = "x"` (public API, and the only way to
> share one policy across classes) still got rows nothing released. See
> **R1** below. The same review caught three stale CLAUDE.md invariants
> the branch had invalidated — one of which instructed the reader to
> reintroduce the leak — and three defects in the branch's own test and
> benchmark tooling. All are fixed on the branch.

### H3 — The Tick pre-inserts inflight rows that only an opt-in deletes

`tick.rb:276-287` inserts one `dispatch_policy_inflight_jobs` row per
admitted job regardless of which gates the policy declares. The only
thing that removes it is `InflightTracker.track`'s `ensure`
(`inflight_tracker.rb:82-86`), which exists only when the job class
called `dispatch_policy_inflight_tracking` (`inflight_tracker.rb:17`) —
an opt-in nothing validates. Two failure modes fall out of the
asymmetry:

**(a) Concurrency gate + a class that forgot the macro → the partition
wedges.** Reproduced: `gate :concurrency, max: 2`, five jobs staged,
thirteen consecutive ticks → two admitted, three never leave. The
partition only unblocks when `inflight_queued_stale_after` (1h) reaps
the rows, then wedges again — an effective limit of "max jobs per
hour". No exception, no warning, nothing in the logs.

**(b) No concurrency gate → one orphan row per admitted job for an
hour.** `README.md:155` states inflight tracking is "only required if a
concurrency gate is used", but the pre-insert happens either way. At
1,000 admits/min that is ~60k dead rows in steady state, and the
dashboard's "in flight" counts jobs that finished long ago.

### H4 — `config.enabled = false` strands the staged backlog

`tick_loop.rb:22-28` breaks out of the loop. Since `around_enqueue_for`
(`job_extension.rb:30`) also sends new enqueues straight to the adapter,
nothing ever looks at the rows already in `staged_jobs` again — they are
reachable only through the UI's drain button. `config.rb:26-31`
documents the opposite: *"Used during cutovers to drain the staging
table without taking traffic offline"*. Reproduced: three staged rows
survive five TickLoop iterations with `enabled = false`.

### H5 — `:adaptive_concurrency` has no upper bound

`repository.rb:829` applies `ELSE current_max + 1` on every healthy
perform, with no check that the cap is the binding constraint, and the
gate accepts no `max:` (`adaptive_concurrency.rb:32-62`). Reproduced:
`initial_max: 2` plus 200 healthy observations → `current_max = 202`.
A partition on a slow steady trickle drifts its cap towards the number
of jobs it has ever run, so when the burst the gate exists for finally
arrives it admits everything. Classic AIMD limiters (TCP, Netflix's
concurrency-limits) always carry a ceiling and only grow while
saturated. The `integer` column means the runaway eventually ends in
`PG::NumericValueOutOfRange`.

## Medium

### M10 — Partitions holding only future-scheduled jobs are re-claimed every tick, forever

`claim_partitions` selects on `pending_count > 0` (which counts
future-scheduled rows) while `claim_staged_jobs!` filters
`scheduled_at <= now()` (`repository.rb:216`). With zero rows claimed,
`tick.rb:236` returns early and `record_partition_admit!` has already
written `next_eligible_at = NULL`, so the partition is immediately
eligible again. Reproduced with a single `set(wait: 1.day)` job: three
ticks, three samples of `{"no_rows_claimed" => 1}`, `next_eligible_at`
nil throughout. Each tick burns a `partition_batch_size` slot and a
full transaction, and the denial breakdown fills with noise.

### M11 — The 24h partition GC silently resets the token bucket

`sweep_inactive_partitions!` (`repository.rb:854`) deletes rows with
`pending_count = 0` after `partition_inactive_after` (24h), and the
bucket lives in that row's `gate_state`. For any throttle whose window
exceeds the cutoff that is a silent quota reset. Reproduced with
`rate: 2, per: 7.days`: two admitted, 25h of simulated idleness, sweep,
then two more admitted inside the same weekly window — four against a
limit of two. `IDEAS.md:19-22` assumes losing the bucket is harmless;
it is harmless only while `per < partition_inactive_after`.

### M12 — `ManualAdmission.force!` wipes a live backoff and skips the fairness decay

`manual_admission.rb:32-38` calls `claim_staged_jobs!` with
`retry_after: nil` and without `half_life_seconds:`. Reproduced: a
partition whose `next_eligible_at` was set by the throttle comes back
with `next_eligible_at = nil` after a UI admit (the tick re-claims it,
re-evaluates and re-backs it off — a wasted cycle), and
`decayed_admits` stays at 1.0 after force-admitting three more jobs.
The second half contradicts the M2 remediation note below
("call `record_partition_admit!` so fairness decay sees manual
admits"): it is called, but without the `half_life_seconds` that
enables the decay clause.

## Low

- **L11** — `stage_many!` upserts partitions in input order
  (`repository.rb:120`); two concurrent `perform_all_later` calls
  touching the same partitions in opposite order can deadlock in
  Postgres. Sorting the groups removes it.
- **L12** — A gate raising inside `evaluate` surfaces as
  `forward_failed` (`tick.rb:324-330`) and leaves `next_eligible_at`
  untouched, so a broken gate is retried and logged on every tick.
- **L13** — `forward_failures` counts partitions (`tick.rb:329`) but
  `operator_hints.rb:92-101` and the views divide it by `jobs_admitted`
  (jobs); the "failure %" is not a ratio.
- **L14** — `decayed_admits_epoch` (`tick.rb:189`) calls `to_time`,
  firing the Rails 8 deprecation on every tick.
- **L15** — `sweep_inactive_partitions!` filters `status = 'active'`
  (`repository.rb:859`), so a paused policy's empty partitions are
  never collected.
- **L16** — Setting `tick_admission_budget` silently makes
  `admission_batch_size` irrelevant as the per-partition ceiling
  (`tick.rb:60-66`), so a transaction can claim far more rows per
  partition than configured. A documentation gap rather than a defect:
  applying `min()` would cap throughput when few partitions are claimed
  (pass-2 makes a single redistribution pass).
- **L17** — `enqueue_after_transaction_commit` (Rails 7.2) is bypassed
  for policy-managed jobs: the interception halts before `raw_enqueue`.
  Harmless on a single database — staging runs in a savepoint of the
  app's transaction, so a rollback drops it and the tick cannot see
  uncommitted rows — but with `config.database_role` pointing at another
  database the staged row commits independently and the job can run
  before the app transaction commits.

---

# Review of the Phase 1 branch — all fixed on it

A max-effort review of the H3 fix (and the tooling that shipped with it)
before merge. Recorded because two of these are the same *shape* as the
bug the branch set out to fix, and because the tooling defects are the
kind that hide the next one.

### R1 — The fix's two ends were still keyed on different things

Creation asked the registered POLICY, at tick time
(`Policy#inflight_tracked_gate`); release asked the CLASS, at macro time
(the `dispatch_policy` call site installed the `around_perform`). Any
other binding — `registry.register(policy)` +
`Job.dispatch_policy_name = "x"`, which is public API, the only way to
point two classes at one policy (a second macro call raises
`PolicyAlreadyRegistered`), and the pattern several cases in this suite
use — got rows created and never released: the original wedge, reachable
through `ActiveJob.perform_all_later`, whose `stageable?` asks for
nothing but a registered policy name.

Fixed by making the include the installation: `InflightTracker`'s
`included` block registers the callback, `JobExtension` declares it as a
Concern dependency, and `track` decides per job from the registry. The
macro survives as a flag that ADDS tracking for gate-less policies.

### R2 — `track` returned before its `ensure` when the policy was unknown

A worker whose registry lacks the policy (renamed or removed while an
older tick was still admitting) stranded the row that tick had
pre-inserted, holding a concurrency slot until the 1h sweeper. The
DELETE keys on `active_job_id` alone, so it now runs regardless.

### R3 — `ManualAdmission` read "policy not in this registry" as "no gate"

The web process's registry is populated as a side effect of job classes
loading, so under lazy loading — or a dashboard-only deployment — a
policy the workers know perfectly well is absent there. Skipping the
pre-insert in that case under-counts the gate and over-admits. It now
inserts unless it knows there is no tracked gate, and warns.

### R4 — Per-row recompute of a constant key, inside the admission TX

The pre-insert recomputed `inflight_partition_key` for every admitted
row: a deep context copy, a mutex-guarded registry fetch and a user proc
call, to arrive back at the partition key the caller already held (both
gates key on `policy.partition_for(ctx)` because `partition_by` is
policy-level). Up to 5,000 recomputes per tick at default batch sizes,
and the branch had just extended the cost to adaptive-only policies,
which previously took the free path.

### R5 — `rake bench:all` ran zero benchmarks and exited 0

`run_all.rb` read its filter from `ARGV.first`, which under rake is the
task name, so every script was filtered out. The CI job added in the
same branch inherited it — a green build that benchmarked nothing, which
is the exact failure mode that job exists to catch. `FILTER` (documented
in the Rakefile) was dead for the same reason.

### R6 — `RUNS=1` crashed every benchmark

Wiring `RUNS` up made small values reachable for the first time, and the
same commit dropped the guard covering them: one sample minus the
discarded warmup left an empty array, and `median` then evaluated
`nil + nil`, minutes into a run.

### R7 — A failed connect disabled every remaining integration test

The shared bootstrap memoized failure as readily as success, where the
ten per-class copies it replaced memoized only success. One transient
hiccup would skip the rest of the suite and — outside CI, where
`DISPATCH_POLICY_REQUIRE_DB` makes it fatal — report green having run
only the unit tests.

### R9 — A class could be bound to a policy for one enqueue API but not the other

Found while writing R1's regression test. `around_enqueue` was installed
by the `dispatch_policy` macro, but `BulkEnqueue.stageable?` asks only
for a registered policy name — so a class bound with
`dispatch_policy_name = "x"` was admission-controlled through
`ActiveJob.perform_all_later` and bypassed admission entirely through
`perform_later`. One job class, two answers, decided by which API the
caller happened to reach for; the throttle or concurrency cap silently
does not apply to half of them.

Same shape and same fix as R1: the callback is installed by
`JobExtension`'s `included` block, and `around_enqueue_for` decides from
the policy at enqueue time (it already returned the job to the adapter
when there was none). Two integration cases could then drop their own
hand-written `around_enqueue`, which is a small proof the global install
covers what the macro used to. A `dispatch_policy_name` check now
short-circuits ahead of the registry lookup, so jobs with no policy
don't take the registry mutex on every enqueue.

### R8 — Three CLAUDE.md invariants contradicted the new code

Including "**`ManualAdmission.force!` also pre-inserts inflight rows**
… Don't remove it", which a future session following literally would
have used to reintroduce the leak. Also a stale "Adding a table?"
pointer at a list the branch had deleted — with both new copies
deferring to that workflow as their sync mechanism — and an overbroad
"nothing touches `inflight_jobs`" claim the same file contradicts 80
lines later.

---

# Remediation plan — audit 2026-08-13

One branch/PR per phase, in this order. Every fix lands with a
regression test. **No phase requires a schema change**, so there are no
upgrade notes for existing installs.

## Phase 1 — H3: inflight row lifecycle *(done — see R1, the deeper version)*

The root cause is that creation is unconditional while deletion is
opt-in. Close both ends:

1. `JobExtension.dispatch_policy` auto-installs the `around_perform`
   when the policy declares a gate from
   `InflightTracker::TRACKED_GATES` (`:concurrency`,
   `:adaptive_concurrency`). Idempotency via a
   `dispatch_policy_inflight_tracking_installed` class attribute that
   the public macro checks too — otherwise a job declaring both would
   nest `track` and record two adaptive observations per perform.
2. The railtie includes `DispatchPolicy::InflightTracker` into
   `ActiveJob::Base` alongside `JobExtension`, so the macro exists
   everywhere. Hosts' explicit `include` becomes redundant, not wrong.
3. `Tick#admit_partition` and `ManualAdmission.force!` pre-insert only
   for policies with a tracked gate. Without one, nobody reads the
   table and the row is pure garbage.
4. Update the CLAUDE.md invariant ("Every admitted job creates a row in
   `inflight_jobs`") and the README's "only required if…" note.

Tests: a concurrency policy whose job never calls the macro drains
fully; a throttle-only policy creates no inflight rows on admit;
declaring the macro on top of the auto-install runs `track` exactly
once.

## Phase 2 — H4: the master switch stops staging, not draining *(done)*

Drop the `break` in `tick_loop.rb`. Final semantics: `enabled` governs
enqueue interception only; work already staged keeps draining. Stopping
admission outright already has two better mechanisms — stop the
`DispatchTickLoopJob`, or pause the policy from the UI (which
`claim_partitions` honors). Fix the `config.rb` comment, document
`enabled` in the README (it appears only in a CHANGELOG line today) and
flag the behavior change in the CHANGELOG.

## Phase 3 — H5: ceiling for `:adaptive_concurrency` *(done)*

1. New `max:` option, defaulting to `initial_max * 10`; validate
   `max >= initial_max`.
2. `repository.rb`: wrap the CASE in
   `LEAST($max, GREATEST($min, …))`.
3. Clamp on read in `evaluate` as well, for rows written before the
   change.

The refinement "only grow while `in_flight >= current_max * 0.8`" needs
the live in-flight count at observation time; record it in `IDEAS.md`
rather than widening this PR.

## Phase 4 — M10: back off to the next `scheduled_at` *(done)*

New `Repository.defer_partition_to_next_scheduled!` issuing a single
`UPDATE … SET next_eligible_at = (SELECT MIN(scheduled_at) … WHERE
scheduled_at > now()) WHERE … AND next_eligible_at IS NULL`, called
from the `rows.empty?` branch inside the same transaction. The
`IS NULL` guard keeps it from stomping a backoff a gate just set; a
NULL subquery result (another tick took the rows) correctly leaves the
partition immediately eligible. Uses `idx_dp_staged_admission` and only
runs when the claim came back empty.

## Phase 5 — M11: the GC must not drop a bucket that is still spending *(done)*

A partition may only be deleted once its bucket would have refilled,
i.e. `last_admit_at + per` — which is exactly what a per-policy cutoff
expresses, since the sweep already keys on `last_admit_at`.

1. `Gates::Throttle` exposes `static_per` (nil when `per` is a proc).
2. `sweep_inactive_partitions!` accepts `policy_name:` /
   `except_policies:`.
3. `TickLoop.sweep!` walks the registry using
   `max(partition_inactive_after, static_per)` per policy, plus one
   catch-all pass for unregistered policy names. N+1 DELETEs every 50
   ticks, N = number of policies.
4. A dynamic `per` cannot be bounded: keep the default cutoff and warn
   once at boot. Correct the claim in `IDEAS.md:19-22`.

## Phase 6 — M12: manual admission *(done)*

Add `preserve_next_eligible:` to `record_partition_admit!` /
`claim_staged_jobs!` (when true the SET becomes
`next_eligible_at = next_eligible_at`; the Tick keeps clearing it,
which is right after a successful admit), and have
`ManualAdmission.force!` pass it along with the policy's
`half_life_seconds`.

## Phase 7 — L11–L15 cleanup, one PR *(done, plus L16/L17 as docs)*

Sort the groups in `stage_many!`; give the `admit_partition` rescue a
short `config.forward_failure_backoff` (default 5s) pushed through
`pending_denies`; feed `partitions_admitted` to `OperatorHints` so the
failure ratio compares like with like; drop the `to_time` call in
`decayed_admits_epoch`; drop `status = 'active'` from the partition
sweep. L16 and L17 are documentation only (README).

---

# Audit 2026-06-10 — all fixed, shipped in 0.5.0

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

# Remediation plan — audit 2026-06-10 (high + medium)

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
