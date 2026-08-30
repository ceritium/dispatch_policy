# Changelog

## Unreleased

### Upgrade notes

- **New columns**: `dispatch_policy_partitions.scheduled_eligible_at`,
  and `failed_at` / `failure_reason` on `dispatch_policy_staged_jobs`
  (all nullable). The gem ships a single migration, so an existing
  install does not get it from `db:migrate` — run it yourself:

  ```sql
  ALTER TABLE dispatch_policy_partitions
    ADD COLUMN scheduled_eligible_at timestamp(6) without time zone;

  ALTER TABLE dispatch_policy_staged_jobs
    ADD COLUMN failed_at timestamp(6) without time zone,
    ADD COLUMN failure_reason character varying;

  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dp_staged_claim_order
    ON dispatch_policy_staged_jobs
    (policy_name, partition_key, priority, scheduled_at ASC NULLS FIRST, id ASC);

  CREATE INDEX CONCURRENTLY idx_dp_partitions_scheduled_order
    ON dispatch_policy_partitions
    (policy_name, shard, status, scheduled_eligible_at NULLS FIRST,
     last_checked_at NULLS FIRST);
  ```

  The type is what `t.datetime` emits under the supported Rails versions,
  so an upgraded install matches a fresh one; using `timestamptz` instead
  works at runtime but leaves this one column disagreeing with the eight
  others on the table and with every fresh install's schema dump. The
  `idx_dp_staged_claim_order` matters once any single partition holds a
  deep backlog: the claim orders by `priority`, which no existing index
  covers, so it sorts the partition's whole backlog on every admission —
  measured at 118 ms and 13.7 MB of temp files to return 200 ids from a
  500k-row partition, twice per tick. Both CREATE INDEX statements are
  CONCURRENTLY because `dispatch_policy_staged_jobs` is the write-hot
  enqueue-path table; run them outside a transaction. Do not drop
  `idx_dp_staged_admission` in favour of the new one — the scheduled-work
  park needs `scheduled_at` third and the new index cannot serve it.

  The partitions index matters once the table is large and most of the work is
  `set(wait:)`-scheduled: `claim_partitions` filters on both horizons, and
  without it the parked rows are eliminated by a heap filter. It is not
  free — the claim rewrites `last_checked_at` on every pass, so the table
  now maintains two indexes on that hot path; an install whose work is
  mostly due-now can skip it.

  No backfill is needed. NULL means "nothing is holding this partition
  back", which is the correct reading for every existing row; a
  partition that was parked under the old scheme carries its horizon in
  `next_eligible_at` and simply becomes claimable one tick earlier,
  after which the tick re-parks it in the new column.
### Fixed

- **The documented separate-queue-database install can admit jobs.**
  `Repository.with_connection` opened its role on `ActiveRecord::Base`,
  which swaps the role for every class in that hierarchy — the host's own
  models included — for the duration of the block. On the multi-database
  setup the README describes (solid_queue on its own database, gem tables
  migrated there, `config.database_role = :queue`) that moved the whole
  process onto the queue database while the adapter still wrote through
  its own record class on its own connection: the admission transaction
  and the adapter's INSERT were never on one connection, so the
  at-least-once guarantee the whole design exists for did not hold, and
  on stock Rails the first `perform_later` raised outright.

  The gem now has a connection identity of its own:
  `config.database_connection_class` names the class it opens on — the
  adapter's record class on a multi-DB install (`"SolidQueue::Record"`,
  or good_job's `active_record_parent_class`), `ActiveRecord::Base` by
  default. `with_connection` scopes the role swap to that class instead
  of the global hierarchy, and the four remaining hard-coded
  `ActiveRecord::Base` entry points — the admission transaction, the
  forced-admission transaction, the forwarder's savepoint and the
  heartbeat's connection release — go through it. Verified against two
  real databases: staging lands in the queue database, the host's own
  connection is untouched while the gem works, and an INSERT through the
  adapter's class inside the admission transaction rolls back with it.
  The railtie warns at boot when it can see the adapter's record class
  differ from the one the gem opens on.

- **One undeliverable staged row no longer wedges its whole partition
  forever.** `Forwarder.dispatch` deserializes every row of a batch
  before enqueuing any of them, inside the admission transaction, so a
  `job_class` the process cannot resolve — a deploy renamed it, dropped
  it, or moved it into a component the tick does not load — rolled the
  whole batch back. That rollback is correct; it is the at-least-once
  guarantee. What was not is what followed: the claim orders by priority
  then id, so the same row headed every subsequent batch forever and the
  healthy jobs behind it in that partition were never admitted again.
  Nothing else deletes from `dispatch_policy_staged_jobs`, there is no
  staged retention sweep, and the partition sweeper keeps a partition
  that still has rows — so the only exit was hand-written SQL. The drain
  button could not get past it either, and `admit` had no rescue at all,
  so it answered with a bare 500.

  Such a row is now quarantined rather than retried: marked with
  `failed_at` / `failure_reason`, skipped by the claim, taken out of
  `pending_count`, and listed on the partition page under "Undeliverable"
  with its reason. Marked, not deleted — dropping a staged job silently
  would break at-least-once — and the hold EXPIRES: `TickLoop.sweep!`
  releases anything older than `config.quarantine_retry_after` (1 hour by
  default), because the ordinary trigger is a rolling deploy whose tick
  pod cannot yet resolve a class the web pods already stage for. A
  terminal hold would turn that deploy into a silent, permanent drop of
  that class's whole backlog — worse than the visible, self-healing stall
  it replaced. A class that really is gone simply re-quarantines. Only an
  unresolvable `job_class` triggers it (`NoMethodError` is a `NameError`,
  so a custom argument serializer touching a nil used to land in the same
  bucket), the dashboard counts held-back rows in their own tile rather
  than as backlog, and an operator hint names them.

  Put them back sooner with the Requeue button on the
  partition page (`Repository.requeue_quarantined_jobs!`), which restores
  `pending_count` in the same statement — clearing `failed_at` by hand
  leaves the row deliverable and unclaimable at once, since the tick only
  claims partitions with pending work. The scheduled park ignores
  quarantined rows too, and the partition sweeper will not collect a
  partition that still holds any, which would otherwise orphan them. The admission retries once after
  quarantining, so the healthy rows in the same batch go out on the same
  tick, and the denial reason is `undeliverable_job` rather than
  `forward_failed`, which pointed at the adapter.

- **The deny flush no longer deadlocks against bulk enqueues.**
  `bulk_record_partition_denies!` is one `UPDATE … FROM (VALUES …)`, and
  one statement gives no lock-ordering guarantee: the planner joins the
  VALUES list against a sequential scan, so it takes row locks in heap
  order — unrelated to `(policy_name, partition_key)` and unrelated to
  the order of the list, so sorting the Ruby array does not help.
  `stage_many!` sorts its per-partition upserts precisely so concurrent
  bulk enqueues agree on an order; against that, the deny crossed.
  Measured with no misconfiguration at all — one tick loop for a policy,
  default config, plus one process calling `perform_all_later` over the
  same partitions: **16 Postgres deadlocks in 20 seconds**. Seven aborted
  the caller's bulk enqueue, leaving the staged half rolled back while
  the non-policy half had already reached the adapter — a batch that is
  not whole-retryable. The other nine killed `Tick#flush_denies!`, which
  rescues and only logs, so every denied partition in that tick lost both
  its backoff and its `gate_state` patch and was immediately
  re-claimable: the M4 busy-loop, tick-wide. The flush now takes its row
  locks up front in the same canonical order `stage_many!` uses. A
  deadlock-retry wrapper would not have done — it leaves the lock convoy
  (bulk enqueue throughput measured ~4× lower) and still surfaces partial
  batches once the retry budget is spent.

- **Introducing or changing `shard_by` no longer strands every existing
  partition.** The shard was pinned on first write and never rewritten,
  so the day a policy gained a `shard_by` — the documented way to
  parallelise a policy across worker pools — every partition that already
  existed kept `default` while the tick loops were started for the new
  shard names. `claim_partitions` filters on shard, nothing rewrites it,
  and new tenants get the new shard and drain normally, so the dashboard
  looks healthy while every existing tenant goes silent, permanently. The
  pin now applies only while the partition holds work: a drained
  partition re-shards on its next enqueue, which is the normal state
  between bursts. A partition stranded while `pending_count > 0` (a
  deploy landing mid-burst) still needs a manual `UPDATE`; that limit is
  documented rather than papered over. CLAUDE.md described this clause as
  `COALESCE(EXCLUDED.shard, partitions.shard)`, which is not what the code
  did and would not have had this behaviour either; corrected.

- **Editing `partition_by` no longer silently removes the concurrency
  cap.** Both concurrency gates counted in-flight rows under a key
  recomputed from the context at evaluate time, while everything on the
  admission path — the staged row, the pre-inserted inflight row, the
  token bucket — is filed under the partition row's own key. The two
  agree until somebody renames or coarsens the expression, which is an
  ordinary deploy; from then on the gate counts zero for every partition
  that predates the edit and hands out the full cap on top of whatever is
  already running. The gates read the row now — and the adaptive gate's
  observations, written at perform time where there is no partition row
  in hand, take their key from the inflight row the Tick pre-inserted, so
  the state it writes and the state it reads cannot drift apart either.
  CLAUDE.md described the two values as identical "by construction",
  which is what kept this invisible for three audits; corrected.

- **A concurrency cap that came back through jsonb no longer wedges the
  partition.** `Gates::Concurrency` resolved its `max:` with `Integer()`,
  but the context a tick evaluates has been through a jsonb round trip,
  which retypes anything that is not a JSON primitive. The README's own
  example backs the cap with a host database column, and a `numeric`
  column arrives as the String `"5.0"` — on which `Integer()` raises,
  inside the admission transaction, so the partition backs off and
  repeats forever. Resolved with `Float().floor`, mirroring what the
  throttle already does with its rate.

- **One poisoned staged row no longer kills the whole drain.** The UI's
  drain buttons called `ManualAdmission.force!` with no error isolation,
  so a staged row the Forwarder cannot deserialize — a job class renamed
  or deleted in a deploy while its rows are still staged — raised
  `NameError` out of the controller as a bare 500: no flash, no partition
  name, no count, and nothing drained. In the policy-wide drain the
  poison partition sorts first, so every healthy partition behind it was
  never reached, identically on every click, leaving the operator's
  escape hatch permanently dead for that policy. Each partition is now
  isolated the way `Tick#admit_partition` already isolates the automatic
  path: the batch is rescued and logged, that partition is abandoned
  (retrying would spin on the same head-of-queue row), and the flash says
  how many partitions could not be forwarded. The raise inside
  `Forwarder`/`ManualAdmission` is untouched — it is what rolls the claim
  transaction back and saves the staged rows.

- **A backoff is no longer parsed out of a string, so a large drain
  cannot take a whole policy's deny bookkeeping with it.** Both backoff
  clauses built `now() + (n || ' seconds')::interval`, and Postgres'
  interval input parser rejects a seconds field above `INT_MAX`. A
  backoff is derived from a token debt, which has no such bound now that
  a forced admission charges the bucket: `retry_after = (1 - tokens) /
  refill_rate` crosses `INT_MAX` after roughly `2.147e9 × rate/per`
  jobs — about 7,100 for a `rate: 2, per: 7.days` policy, inside a
  single drain click. `bulk_record_partition_denies!` builds ONE
  statement for the whole tick and `Tick#flush_denies!` only logs on
  failure, so one unparseable interval discarded every denied
  partition's `next_eligible_at` **and** `gate_state` patch in that
  batch. Those partitions were then re-claimed on every tick with
  nothing recorded — the M4 busy-loop, for a whole policy, from one UI
  click, silent but for a single log line. Both clauses multiply an
  interval instead — and clamp first, at `MAX_BACKOFF_SECONDS` (1e9, about
  31 years). The multiply raises the ceiling ~4295x but does not remove
  it: `interval` stores microseconds in an int64, so it still raises past
  ~9.22e12 seconds, which a `rate: 1, per: 1.year` policy reaches at
  ~292k drained jobs. A backoff longer than the clamp is not a backoff
  anyone will outlive anyway.

- **A job that dies before `around_perform` releases its slot even
  without `discard_on`.** The railtie reaped the Tick's pre-inserted
  inflight row on `discard.active_job`, and CLAUDE.md claimed that
  covered every job killed before the perform callbacks. It does not:
  ActiveJob instruments `discard` in exactly one place — the
  `rescue_from` handler `discard_on` installs — so a job class with no
  handler dies in `perform_now`'s bare `rescue Exception` and emits
  nothing at all. The routine case is a GlobalID argument whose record
  was deleted between enqueue and perform, raising
  `ActiveJob::DeserializationError` during argument deserialization: the
  row then orphaned until the `inflight_queued_stale_after` sweeper an
  hour later, and with `gate :concurrency, max: 1` that is an hour of a
  frozen tenant per such job. The railtie now also subscribes to
  `perform.active_job` and reaps when the payload carries an exception —
  idempotent, since the normal path has already deleted the row, and safe
  against a late delete because the Tick regenerates `active_job_id` on
  every admission. The rule lives in
  `InflightTracker.handle_failed_perform` rather than in the initializer
  block, because a subscription body is unreachable from the suite.

  Relatedly, a job whose arguments cannot be rebuilt no longer raises out
  of the *enqueue* callback, on either path — the bulk one materializes
  before it splits the batch, so one such job is routed to the adapter
  rather than aborting the row builder and losing every other stageable
  job with it (after the non-policy half has already gone to the
  adapter, where a caller that rescues and re-drives would duplicate
  them). ActiveJob's own enqueue copes (`serialize`
  reuses the serialized arguments), so raising there destroyed the retry
  that `retry_on ActiveJob::DeserializationError` had just scheduled —
  turning a recoverable job into a hard failure the gem itself caused.
  Such a job is handed to the adapter instead and fails at perform, which
  is what would have happened without the gem.

- **The generated tick-loop job no longer dies on its first iteration
  under solid_queue.** The template's shutdown check called
  `SolidQueue::Process.current_process`, a method solid_queue has never
  had (the string does not appear anywhere in 1.3, 1.4 or 1.7).
  `defined?(SolidQueue::Process)` is truthy once solid_queue is loaded,
  so the call always raised `NoMethodError` — and it is the `stop_when`
  lambda, evaluated as the first statement inside the loop, outside the
  rescue that guards `Tick.run`. The exception escaped `perform`, the
  `set(wait: 1.second).perform_later` successor was never enqueued, and
  the chain was dead after one run: every install on solid_queue staged
  jobs that nothing ever admitted. Both supported adapters implement the
  ActiveJob `stopping?` hook, so the per-adapter branch is gone. As
  defence in depth, a `stop_when` that raises is now logged and treated
  as "not stopping" rather than taking the loop down.

- **Staged jobs are admitted in the priority order ActiveJob means.** The
  claim ordered `priority DESC`, i.e. the largest number first, while
  both supported adapters define the opposite — good_job's
  `priority_ordered` is `priority ASC NULLS LAST`, solid_queue's
  `ordered` is `priority: :asc` — and the enqueue path stores
  `job.priority` verbatim. A host setting `priority: -10` for interactive
  work and `priority: 10` for bulk therefore had it exactly backwards,
  and behind a steady stream of default-priority jobs the urgent one was
  starved indefinitely. The dashboard's staged list mirrored the same
  inverted order, so the UI confirmed it rather than exposing it. Both
  now sort ascending, and the README states the convention.

- **`dispatch_policy_adaptive_concurrency_stats` is garbage-collected.**
  `adaptive_seed!` runs on every evaluate of every partition and nothing
  in the gem ever deleted from that table, so its row count was "every
  partition key this policy has ever seen" — unbounded for the
  per-tenant/per-user/per-endpoint `partition_by` the README recommends,
  with no knob, no dashboard surface and no rake task. `TickLoop.sweep!`
  now collects stats whose partition is already gone, reusing
  `partition_inactive_after`: the partition row is the gem's authority on
  liveness and its own sweeper already respects a throttle's refill
  window, so no new retention setting is needed. Re-seeding a partition
  that comes back costs one `ON CONFLICT DO NOTHING` insert at
  `initial_max`.

- **A job class that defers its own enqueue no longer breaks admission.**
  ActiveJob 7.2+ lets a class set `self.enqueue_after_transaction_commit
  = true` — the setting Rails recommends for apps that enqueue inside
  ActiveRecord transactions. `Forwarder.dispatch` runs INSIDE the
  admission transaction, so that deferral registered the real enqueue on
  the gem's own transaction and it landed after COMMIT, outside the
  `Bypass` window: the scheduled path re-staged the job it had just
  admitted, on every tick forever, leaking one inflight row each time
  (so a `:concurrency` gate wedged at `max` within `max` ticks), and the
  immediate path saw `successfully_enqueued? == false`, raised, and
  rolled the whole admission back on every tick forever. The job never
  reached the adapter either way and nothing said so. The enqueue now
  runs inside a non-joinable savepoint when a job in the batch defers,
  which is what makes `ActiveRecord.after_all_transactions_commit` run
  its block inline — the work happens inside the admission TX and inside
  Bypass, as the contract requires. Deployments with no such job class
  are unaffected. `transaction` swallows `ActiveRecord::Rollback` by
  design, so the savepoint re-raises: absorbing one would let the
  admission commit with the staged rows deleted and nothing in the
  adapter, where the non-deferring path aborts.

- **The periodic sweeper actually runs.** `TickLoop.run` counted
  iterations in a local, but the generated `DispatchTickLoopJob` calls
  `run` for one bounded window (`tick_max_duration`) and re-enqueues
  itself — so the counter restarted every window. With the shipped
  defaults a window holds at most `tick_max_duration / idle_pause` =
  25 / 0.5 = 50 iterations, exactly `sweep_every_ticks`, so any
  per-iteration cost at all left it at 49 and `iteration %
  sweep_every_ticks` never hit zero. Nothing was ever swept: stale
  inflight rows wedged a `:concurrency` partition permanently (the gate
  counts rows nothing reaps, admitting 0 means `idle_pause`, and
  `idle_pause` is what keeps the window under 50 — the wedge feeds
  itself), and `dispatch_policy_tick_samples` grew without bound. The
  repo's own dummy log shows the symptom: 53,835 `claim_partitions`
  against 215 `sweep_stale_inflight`, a 1:250 ratio where the nominal
  cadence is 1:50. The counter now lives on the module and survives
  re-entry.

- **The heartbeat thread no longer leaks a database connection per
  running job.** `InflightTracker`'s heartbeat wrapped its UPDATE in
  `connection_pool.with_connection`, believing that borrowed and returned
  a connection per beat. It does not: the thread is a bare `Thread.new`
  running outside the Rails executor, so nothing has established a lease
  for it and the pool treats the lease as PERMANENT — `with_connection`
  marks it sticky and its ensure then skips `release_connection`, on the
  assumption that whoever established the lease will release it. Nothing
  did. The first beat pinned a connection to the heartbeat thread for the
  rest of the job, and when the thread died the connection was not
  returned either: it sat checked out with a dead owner until the pool
  reaper got to it.

  With the Rails default sizing — pool size and worker threads both from
  `RAILS_MAX_THREADS` — every tracked job that outlives one
  `inflight_heartbeat_interval` doubles its connection demand, so a full
  worker raises `ActiveRecord::ConnectionTimeoutError` on the jobs
  themselves. The beats that lose the race fail too, which stalls
  `heartbeat_at` on rows whose jobs are still running, and the stale
  sweep then reclaims them after `inflight_stale_after` — leaving the
  concurrency gate under-counting and over-admitting. Verified against
  Rails 8.1: after `with_connection` returned inside a thread the pool
  still reported the connection busy, and it went to `dead` rather than
  `idle` when the thread exited. The beat now releases explicitly — and
  from inside `Repository.with_connection`, since `connected_to` is
  block-scoped and releasing after it returns aims at the writing pool
  while the lease belongs to the role's.

- **The throttle's token bucket is charged atomically** (throttle
  review). It was written back as a literal jsonb patch computed in Ruby
  from an earlier read — a read-modify-write across two statements. Two
  tick loops covering the same `(policy, shard)` each evaluated a full
  bucket, each admitted it, and the second write overwrote the first, so
  one admission went uncharged and the effective rate became
  `rate × loops` **indefinitely**. Reproduced against Postgres: 20 jobs
  admitted against `rate: 10, per: 60`, bucket left at `0.0` instead of
  `-10`. The bucket is now recomputed inside the admission UPDATE from
  the row's own value, so concurrent charges compose and the overdraft is
  repaid out of the next window — the long-run rate holds. Note this
  makes the *charge* atomic, not the admission *decision*: a transient
  burst is still possible, so one tick loop per `(policy, shard)` remains
  the recommendation. `evaluate` also stops persisting its refill, which
  is recomputable and, on the deny path, could overwrite a concurrent
  admission's charge.

  The bucket stays on ONE clock while doing so: the charge reads the
  token count from the row but takes its timestamps from
  `DispatchPolicy.config.now`, bound as parameters. Settling against
  Postgres `now()` would put the two ends of one subtraction on two
  clocks — `evaluate` refills from `config.now`, so any offset between
  app and database is credited as free tokens on every evaluate — and
  `now()` is the transaction timestamp, which stops advancing inside an
  enclosing transaction. The stamp is written as `GREATEST(now, stored)`
  so that two transactions committing out of order cannot rewind it and
  refill the same interval twice.

- **A job due now no longer waits behind a future-scheduled one.** M10
  parked a partition holding only future work by writing
  `next_eligible_at`, the same column gates use for their backoff.
  Nothing on the enqueue path could then clear it — clearing it there
  would clobber a gate's backoff and bring back the busy-loop that
  backoff exists to prevent — so a `perform_later` landing behind a
  `set(wait: 1.hour)` sat in `dispatch_policy_staged_jobs` for the full
  hour, unclaimed, with no gate having denied it and nothing in the
  logs. With `wait: 1.week` it waited a week. Reproduced:
  `partitions_seen = 0` on five consecutive ticks with two staged rows,
  one of them due. The horizon now lives in its own column,
  `scheduled_eligible_at`, and `claim_partitions` requires both. A job
  due now clears it; a future job cannot install one over a partition
  that already has due work. Both enqueue paths maintain it — the bulk
  one (`perform_all_later` → `stage_many!`) upserts once per partition
  for a whole batch, and one job in that batch being due settles it for
  the group. The park itself asks "is anything due?" in the same
  statement and snapshot as the write, so a job that becomes due between
  the claim and the park — an enqueue committing while the tick waits on
  the partition row lock — cannot be hidden behind a horizon the park
  never saw it beside.

  Side effect worth knowing: the horizon is set by the enqueue itself,
  so a partition holding only future work is no longer claimed even
  once. Ticks that used to spend a claim slot discovering there was
  nothing to do now skip it entirely.

- **The partition sweeper holds a throttled partition until its bucket
  has refilled, instead of for a whole window.** Retaining the row for
  `per` was an approximation of "the bucket is back at capacity", and it
  was wrong in both directions. Too long: a `per: 7.days` policy kept
  every partition for a week, when a bucket that spent one of two tokens
  is full again in 3.5 days. Too short: a bucket left in debt by
  concurrent loops needs more than one window to climb back, and a
  sub-unit rate (`rate: 0.5, per: 7.days` — capacity is floored at one
  token while the refill runs at the true rate) needs two — collecting
  either hands the tenant a fresh quota, which is the reset the window
  rule existed to prevent. The sweeper now refills the stored bucket to
  now with the same expression the admission UPDATE uses and collects
  only what has genuinely reached capacity. Applies when both throttle
  knobs are fixed numbers; a proc `rate` or `per` still falls back to
  the window cutoff.

- **A forced admission charges the throttle.** `ManualAdmission.force!`
  — the dashboard's admit/drain buttons — bypasses every gate by design,
  but it was also escaping the token bucket's cost: a drain of N jobs
  went out with the bucket untouched, so the tenant received N plus a
  whole fresh window and the rate the policy declares stopped being true.
  The bucket now goes into debt by exactly what was forwarded, and the
  next window repays it — the same overdraft two racing tick loops
  produce. Operators should expect a large drain to leave the partition
  quiet for a while; that is the rate contract catching up, not a stall.
  Only a fixed `rate`/`per` can be charged from that path (a proc needs
  the partition's context, which the web process never loads there); a
  dynamic throttle is left alone and logs a warning.

- **The catch-all sweep no longer resets the token bucket of a policy
  this process merely hasn't loaded.** `TickLoop.sweep!` collects
  partitions whose policy is absent from `DispatchPolicy.registry`,
  reading that as "deleted from the code". The registry is populated as
  a side effect of job classes loading, so it is also every policy a
  dashboard-only process, a lazily-loaded worker or a half-rolled deploy
  has not touched — the same trap ISSUES.md R3 records for
  `ManualAdmission`. Reproduced with `rate: 2, per: 7.days`: the row a
  worker that knows the policy correctly keeps is deleted by one that
  does not, and the tenant gets two more admits inside the same week. A
  row that still carries a token bucket now waits out the new
  `config.unknown_policy_retention` (30 days by default, long enough to
  cover any plausible window) instead of `partition_inactive_after`;
  a row with nothing to lose is still collected on the usual cutoff, so
  a genuinely deleted policy is still garbage-collected.

- **Inflight rows are no longer created without a way to release them**
  (audit 2026-08-13, H3). Admission pre-inserted a row in
  `dispatch_policy_inflight_jobs` for every job it let through, while
  the only thing that deletes those rows — `InflightTracker.track`'s
  `around_perform` — had to be opted into per job class with
  `dispatch_policy_inflight_tracking`. Two consequences, both silent:

  - a policy with a `:concurrency` / `:adaptive_concurrency` gate whose
    job class forgot the macro **wedged**: the gate counted rows nobody
    removed, so the partition stopped admitting at `max` until the
    `inflight_queued_stale_after` sweeper (1h) reclaimed them, then
    wedged again;
  - a policy *without* such a gate — where the README said the macro
    wasn't needed — leaked one row per admitted job for an hour,
    inflating the dashboard's in-flight count with finished jobs.

  Both ends are now driven by the same fact, read from the registered
  policy at runtime (`Policy#inflight_tracked_gate`):
  `Tick`/`ManualAdmission` create rows for concurrency-family policies,
  and `InflightTracker.track` releases them on the same condition.
  Including `InflightTracker` is what installs the `around_perform`, and
  `JobExtension` brings it along as a Concern dependency, so tracking
  cannot be missing from a class that can be staged — including one
  bound with `dispatch_policy_name = "x"` instead of the
  `dispatch_policy` macro, which is public API and the only way to point
  two classes at one policy.

  `dispatch_policy_inflight_tracking` keeps working and now does one
  thing: it ADDS tracking for a policy with no such gate (the live
  in-flight count on the dashboard). It installs nothing, so forgetting
  it can no longer wedge a partition, and declaring it twice — or
  alongside the railtie's include — still tracks exactly once.

  Two smaller asymmetries in the same lifecycle went with it: a worker
  whose registry no longer has the policy (renamed mid-deploy) now still
  releases the row a tick pre-inserted, and `ManualAdmission` no longer
  skips the pre-insert just because the *web* process's registry hasn't
  loaded that job class — it inserts unless it knows there is no tracked
  gate, and warns.

  No schema change, no action required on upgrade; existing job classes
  keep working unchanged.

- **A job class bound to a policy without the `dispatch_policy` macro is
  now staged by both enqueue APIs** (audit review, R9). `around_enqueue`
  was installed by the macro while `BulkEnqueue.stageable?` asked only
  for a registered policy name, so a class bound with
  `dispatch_policy_name = "x"` went through admission via
  `ActiveJob.perform_all_later` and straight to the adapter via
  `perform_later` — the same job class, with the throttle or concurrency
  cap applying to only half of its enqueues.

- **`:adaptive_concurrency` caps how far `current_max` can grow** (audit
  2026-08-13, H5). AIMD added 1 per healthy perform without checking
  whether the cap was the binding constraint and with no upper bound, so
  a partition on a slow, healthy trickle climbed indefinitely: after 200
  successful performs a gate declared with `initial_max: 2` sat at 202,
  no longer limiting anything by the time the burst it exists for
  arrived — and `current_max` is an integer column, so the drift ends in
  `PG::NumericValueOutOfRange`. New `max:` option, defaulting to
  `initial_max × 10`, applied both in the UPDATE and when the cap is read
  (so a row written by an earlier version can't out-rank the current
  configuration). `max` below `initial_max` raises at policy-definition
  time. Existing policies get the default ceiling without any change; set
  `max:` explicitly if the downstream can take more than 10×.

- **A partition holding only future-scheduled work no longer spins**
  (audit 2026-08-13, M10). `claim_partitions` counts scheduled rows in
  `pending_count` while `claim_staged_jobs!` only takes due ones, so such
  a partition was claimed, found empty and left immediately eligible —
  a transaction and a `partition_batch_size` slot burned every tick until
  the job came due. It now parks on the soonest `scheduled_at`, without
  overwriting a backoff a gate asked for.

- **The partition sweeper no longer resets a token bucket that is still
  spending** (audit 2026-08-13, M11). The bucket lives in the partition
  row's `gate_state`, so collecting the row inside the throttle's refill
  window handed the tenant a fresh quota: `rate: 2, per: 7.days` plus a
  day of quiet admitted two more inside the same week. The sweep is now
  per-policy at `max(partition_inactive_after, window)`, plus a
  catch-all pass for partitions whose policy is no longer registered. A
  dynamic `per` can't be resolved without a context, so those keep the
  default cutoff and log a warning once per process.

- **`ManualAdmission.force!` (UI admit/drain) keeps a gate's backoff and
  feeds the fairness EWMA** (audit 2026-08-13, M12). It passed
  `retry_after: nil`, wiping whatever backoff a gate had set even though
  a forced admission bypasses the gates and has learned nothing about
  capacity — so the next tick re-claimed the partition, re-evaluated it
  and backed it off again. It also never passed `half_life_seconds`, so
  the decay clause was skipped and manual admits stayed invisible to the
  in-tick fairness reorder.

- **A failed admission backs its partition off** (audit 2026-08-13, L12)
  instead of being retried every tick. New `config.forward_failure_backoff`
  (default 5s, 0 disables). Whatever raised — the adapter refusing
  enqueues, a gate with a bug — is not fixed by the next tick moments
  later, and retrying immediately burned a claim slot and a transaction
  per iteration while repeating one error into the log.

- **The forward-failure percentage compares like with like** (audit
  2026-08-13, L13). `forward_failures` counts partitions, but the
  dashboard and the operator hints divided it by `jobs_admitted`: with a
  healthy 100 jobs per partition, *every* partition failing showed up as
  a reassuring 1%. The denominator is now `partitions_seen`.

- **Empty partitions of a paused policy are collected** (audit
  2026-08-13, L15). The sweep required `status = 'active'`, so pausing —
  exactly when partitions go empty and stay that way — meant they
  accumulated forever. The policy-level pause flag lives in its own
  table, so it still applies when the partition reappears.

- **Concurrent `perform_all_later` calls can't deadlock on partition
  upserts** (audit 2026-08-13, L11): the per-partition UPSERTs are now
  issued in a deterministic order, so two batches touching the same
  partitions take their row locks in the same sequence.

- **No more Rails 8 deprecation on every tick** (audit 2026-08-13, L14):
  `decayed_admits_epoch` no longer calls `to_time` on a String.

### Changed

- **`config.enabled = false` no longer stops the tick loop** (audit
  2026-08-13, H4). It turns off *staging* — new `perform_later` calls go
  straight to the adapter — but the loop used to exit as well, which
  stranded everything already in `dispatch_policy_staged_jobs`: nothing
  else hands those rows to the adapter, so the backlog was reachable
  only through the dashboard's drain button. That is the opposite of the
  documented purpose ("drain the staging table without taking traffic
  offline"), which now actually works: flip the flag, watch the backlog
  drain, then stop the tick job. **If you were using `enabled = false` as
  a way to stop admission, it no longer does that** — stop the tick job,
  or pause the policy from the dashboard (the pause flag is what
  `claim_partitions` honors, and it also holds partitions created after
  the pause).

- **The dashboard's in-flight count for a policy with no
  concurrency-family gate now reflects jobs that are actually running**,
  and only when the job class declares `dispatch_policy_inflight_tracking`.
  It previously counted every admission for an hour, including jobs that
  had long since finished. For a tracked policy nothing changes: the
  count still covers the whole admitted-to-finished window, queue wait
  included.

## 0.5.0

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
- The `:throttle` gate's `per` now accepts a lambda (like `rate`), so the
  rate-limit window can depend on per-job context. A resolved `per <= 0`
  raises.
- Policy-level **pause** now actually holds the whole policy. The pause
  flag lives in the new `dispatch_policy_policy_settings` table and is
  honored by `claim_partitions`, so it also stops partitions that first
  appear *after* the pause — previously `pause` only flipped the `status`
  of partition rows that existed at click time, and a tenant's first
  enqueue afterwards created an `active` partition the next tick admitted.
  The per-partition `status` update is kept for the partitions-index
  display; `resume` clears the flag.
- The admin UI now reflects the policy-level pause flag everywhere
  (policies index + show, dashboard policy rows, partitions index + show):
  partitions created after a pause render as effectively paused even
  though their own `status` is still `active`, the pause/resume button
  toggles to a single relevant action, and `policies#show` shows a PAUSED
  badge. The per-policy operator hints also short-circuit to a single
  "policy is paused" note instead of falsely warning about never-checked
  partitions / growing backlog while admission is intentionally stopped.

### Fixed
- **The admin UI honors `config.database_role`.** The engine controllers
  query the gem tables through the AR models directly (`Partition`,
  `StagedJob`, `InflightJob`, `PolicySetting`, `TickSample`), which the
  `Repository` role wrapper doesn't cover — under multi-DB every dashboard
  page queried the default writing role (`PG::UndefinedTable` → 500), and
  `pause`/`resume` updated the partition `status` in the wrong DB while
  the policy flag went to the right one. An `around_action` in the
  engine's `ApplicationController` now wraps every action — including view
  rendering, so lazily-evaluated relations stay routed — in
  `Repository.with_connection`. No-op without `database_role`.
- **`pause`/`resume` write the policy flag and the partition statuses in
  one transaction.** They were two autocommitted statements; a crash
  between them left the partition list contradicting what admission
  actually does until the next toggle.
- **The generated `DispatchTickLoopJob` no longer dies after its first run
  under good_job.** It re-enqueues itself at the end of `perform`, but
  `good_job_control_concurrency_with(total_limit: 1)` counts the
  still-running job in its enqueue check (`unfinished`), so the successor
  was silently aborted and admission stopped after `tick_max_duration`.
  Switched to `enqueue_limit: 1` + `perform_limit: 1` (the enqueue check
  excludes the running job) and the job now logs an error if a re-enqueue
  is ever refused. solid_queue was unaffected.
- **`Tick#record_sample!` routes its two AR-model reads through
  `config.database_role`.** They bypassed the `Repository` role wrapper, so
  under a separate queue DB they queried the wrong role and the swallowed
  error meant no `tick_sample` was ever written (empty dashboard/metrics).
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
- `stage_many!` chunks its INSERT into batches of 1,000 rows so a bulk
  `perform_all_later` larger than ~8,191 jobs no longer blows Postgres's
  65,535 bind-param limit and fails the whole batch.
- `InflightTracker.track` now inserts the inflight row and spawns the
  heartbeat inside its `begin/ensure`, so a failure spawning the heartbeat
  thread can't leave a ghost inflight row behind until the sweeper.
- `Registry` reads (`fetch`/`names`/`each`/`size`) take the same mutex as
  `register`/`clear` (snapshotting before iterating in `each`), removing a
  data race on non-GVL runtimes (JRuby/TruffleRuby).
- The DSL rejects `tick_admission_budget`/`admission_batch_size` of `0` or
  negative (a silent full stop of the policy) and the `concurrency` /
  `adaptive_concurrency` gates reject a negative `full_backoff` (which
  would put `next_eligible_at` in the past and re-evaluate every tick).
  `nil` still defers to config.
- The policy-wide drain passes its remaining budget to each partition so
  the total can't overshoot the 10,000 cap by nearly 2×, and a drain that
  only leaves future-scheduled jobs now says "N scheduled for later
  remain" instead of looping "click drain again" forever.
- `partitions#show` lists recent staged jobs in the real admission order
  (`priority DESC, scheduled_at NULLS FIRST, id`) and drops a dead,
  mis-scoped `@inflight` query.
- `Context` now exposes indifferent (symbol/string) access at every depth,
  not just the top level — `ctx[:limits][:max]` no longer silently returns
  nil when the host wrote a nested hash with symbol keys. `to_jsonb`/`to_h`
  still return the plain string-keyed hash for storage.
- The tick loop survives misconfigured pacing: `sweep_every_ticks <= 0`
  now means "never sweep" instead of raising `ZeroDivisionError`, and a
  negative `idle_pause`/`busy_pause` is treated as no pause instead of
  raising in `sleep`. Both previously escaped the loop's rescues and
  stopped admission.
- Pass-2 budget redistribution denies (e.g. a throttle emptied after
  pass-1) now feed the tick sample's denied-reason breakdown, so the
  dashboard reflects why redistribution stopped.
- Admin UI: `format_count` keeps the sign of negative values; durations
  clamp at 0 so app↔DB clock skew can't render "-340ms"; the partition
  search escapes `%`/`_` so a literal key containing them matches
  literally; and the refresh/theme controls bind via a single delegated
  document listener instead of per-button (Turbo's morph refresh dropped
  the `data-bound` guard, leaking a new listener per refresh).
- Dummy app: the throttle demos (`slow_api`, `mixed`) honor the form's
  `per` field via the new callable `per` instead of a hardcoded window
  (`slow_api` was stuck at 60000s), and the enqueue forms tolerate blank
  numeric fields / unknown job names instead of 500ing.

### Internal
- Corrected the `bulk_record_partition_denies!` comment: `claim_partitions`
  runs autocommitted, so its `FOR UPDATE SKIP LOCKED` locks don't guard the
  end-of-tick deny flush — the one-tick-loop-per-(policy,shard) invariant
  and the `last_checked_at` bump do.

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
