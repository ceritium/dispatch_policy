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

262 runs / 621 assertions. `bundle exec rake test` from the root.

## Architecture — 6 tables

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
dispatch_policy_policy_settings               one row per policy — pause flag
                                              (claim_partitions skips paused policies)
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
  metadata.** The shard is pinned while the partition holds work and
  recomputed once it is drained — `CASE WHEN pending_count = 0 THEN
  EXCLUDED.shard ELSE partitions.shard END` in `upsert_partition!`, where
  `pending_count` is the pre-UPDATE value. That keeps a partition from
  jumping between tick workers mid-claim, while still letting it follow
  `shard_by` when the declaration changes. Pinning it unconditionally —
  which is what the code did, and what this note used to describe, wrongly,
  as `COALESCE(EXCLUDED.shard, partitions.shard)` — stranded every
  pre-existing partition the day `shard_by` was introduced: their rows
  kept a shard no loop was started for, and nothing ever rewrote it.
  A partition stranded while it still holds work does NOT self-heal; only
  a drained one does.
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
  of them". Nothing writes `inflight_jobs` for such a policy unless
  its job class opts in with `dispatch_policy_inflight_tracking`
  (which buys a dashboard count, nothing else — no admission decision
  reads those rows).
- **Two columns park a partition, and they are not interchangeable.**
  `next_eligible_at` is a GATE backoff ("we asked, capacity said wait");
  `scheduled_eligible_at` is the scheduled-work horizon ("there is
  nothing due yet"). `claim_partitions` requires both to be clear.
  Sharing one column was M10's bug: an enqueue cannot lower a gate
  backoff without resurrecting the busy-loop the backoff prevents, so a
  job due NOW stayed invisible behind a `set(wait: 1.week)` sibling for
  a week. `upsert_partition!` maintains the horizon on every enqueue
  and NULL is absorbing in both directions — due work clears it, and a
  future job cannot install one over a partition that already has due
  work.
- **"Not in `DispatchPolicy.registry`" means "we can't see it", NOT "it
  was deleted".** The registry fills as a side effect of job classes
  loading, so a dashboard process, a lazily-loaded worker or a rolling
  deploy all reach code with a policy the rest of the fleet knows.
  `ManualAdmission` learned this once (ISSUES.md R3) and `TickLoop`'s
  catch-all sweep learned it again: collecting such a partition deletes
  its token bucket and hands the tenant a fresh quota. A row carrying a
  bucket now waits out `config.unknown_policy_retention` there. Any new
  code that branches on registry membership has to err the same way.
- **`partitions.context` is refreshed on every `perform_later`** via
  UPSERT. Gates read that ctx, NOT `staged_jobs.context` (which is
  historical). This lets a change in the host DB (e.g. new
  `max_per_account`) take effect on the next enqueue.
- **One tick loop per (policy, shard) — the throttle's burst depends on
  it, not its long-run rate.** `evaluate` reads the token bucket before
  the admission TX opens, so two loops on the same shard can both see a
  full bucket and both admit it. The CHARGE is atomic (the bucket is
  recomputed inside the admission UPDATE from the row's own value), so
  the two costs compose, the bucket goes negative, and the debt is
  repaid out of the next window — the rate holds over time, the burst
  does not. The generated `DispatchTickLoopJob` helps but does not
  guarantee it: its good_job / solid_queue concurrency key is
  `"dispatch_tick_loop:#{policy}:#{shard}"`, so it dedupes an identical
  argument tuple — it does NOT stop a catch-all
  `perform_later` (key `all:all`) from overlapping a
  `perform_later("events")` (key `events:all`), and the no-adapter
  branch of the template has no concurrency control at all.
  Do NOT go back to writing the bucket as a literal jsonb patch computed
  in Ruby: that is a read-modify-write, the second writer wins, and the
  rate silently becomes `rate × N_loops` forever.
- **The token bucket lives on ONE clock: `DispatchPolicy.config.now`.**
  The charge is settled in SQL from the row's own value, but the
  timestamps it reads and writes are bound as parameters from the gate's
  clock — never `EXTRACT(EPOCH FROM now())`. Only the token COUNT has to
  come from the row. `evaluate` refills from `config.now`, so sourcing
  the other end from the database puts one subtraction on two clocks:
  an offset O silently adds `O × refill_rate` phantom tokens to every
  evaluate, permanently. `now()` is also the TRANSACTION timestamp, so
  inside an enclosing transaction (Rails transactional tests, a host
  wrapping the tick) it stops advancing entirely. The stamp is written
  as `GREATEST(now, stored)`: two admission transactions can execute in
  the opposite order to the one they started in, and a stamp that moves
  backwards makes that interval refill twice.
- **The partition sweeper holds a throttled partition until its bucket
  has REFILLED, not until its window is out.** `sweep_inactive_partitions!`
  refills the stored value to now with the same expression the admission
  UPDATE uses (hence `Policy#static_throttle_refill_rate`, which is not
  `capacity / window` — a sub-unit rate floors capacity at one token).
  Comparing the stored snapshot instead would be inert: the admission
  UPDATE is its only writer and always subtracts, so a partition that
  ever admitted anything is frozen below capacity forever. "One window
  has passed" is not the same test in either direction — a bucket in
  debt needs more than a window, a `rate: 0.5` bucket needs two — and
  collecting early is the M11 quota reset.
  (The older warning here — that a too-fine `shard_by` duplicates the
  bucket across shards for `rate × N_shards` — no longer applies: since
  `partition_by` became policy-level, `(policy_name, partition_key)` is
  unique and the shard is pinned on first write, so one partition_key is
  exactly one row on exactly one shard.)
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
- **The forward must not be deferrable.** ActiveJob 7.2+ lets a job class
  set `enqueue_after_transaction_commit = true`, which reroutes its
  enqueue through `ActiveRecord.after_all_transactions_commit` — i.e.
  onto the gem's OWN admission transaction, landing after COMMIT and
  outside `Bypass`. That re-stages the job the tick just admitted (or
  rolls the admission back) forever. `Forwarder.dispatch` therefore wraps
  the enqueue in a NON-JOINABLE savepoint when any job in the batch
  defers: `ActiveRecord.all_open_transactions` skips non-joinable
  transactions, so the deferral finds nothing to wait for and runs
  inline. Only when needed — a non-joinable savepoint runs its commit
  callbacks on RELEASE rather than at the real COMMIT.
- **The sweep cadence counter lives on the module, not in `run`.** The
  generated tick job calls `TickLoop.run` for one `tick_max_duration`
  window and re-enqueues itself, and the shipped defaults put exactly
  `sweep_every_ticks` iterations in a window — so a per-invocation
  counter never reaches the modulo and NOTHING is ever swept. Don't move
  it back into a local, and don't assume a fresh `run` starts a fresh
  cadence.
- **Non-PG adapter = warn at boot, no hard-fail.** The railtie calls
  `DispatchPolicy.warn_unsupported_adapter` in `after_initialize`.
  If the host runs Sidekiq/Resque, a warning explains atomicity is
  lost. Deliberate: a custom PG-backed adapter (not detected) can
  still work, and we don't want to break its deploy.
- **`config.database_connection_class` is the gem's connection
  IDENTITY, and it must be the adapter's.** The at-least-once guarantee
  is that the adapter's INSERT joins the admission TX, which holds only
  while both are on one connection — so `Repository.base_class` is what
  every DB entry point opens on, including the admission transaction, the
  forwarder's savepoint and the heartbeat's release. Do NOT put
  `ActiveRecord::Base` back in any of them.
  `ActiveRecord::Base.connected_to(role:)` is worse than it looks: it
  swaps the role for the whole hierarchy, host models included, and still
  leaves an adapter that writes through its own record class on a
  different connection. That is why the documented separate-queue-DB
  install could not admit a job.
- **`config.database_role`**: for Rails multi-DB (e.g. solid_queue
  with a separate DB), sets the role every Repository call is opened
  against. **All** public `Repository` methods are auto-wrapped in
  `with_connection` (`connected_to(role:)`) at the bottom of
  `repository.rb` — not just the admission TX — so staging, claim,
  inflight counts/tracking, sweeps and dashboard reads all hit the DB
  the gem tables live in. The wrap captures each original as a bound
  closure (no `super`/prepend): a `super`-based prepend stacks wrappers
  and stack-overflows when the suite re-evaluates the file. New public
  Repository methods are routed automatically; pure helpers
  (`normalize_*`, `parse_jsonb`, `sample_filter`,
  `next_eligible_clause`, `trend_direction`) and the `connection`
  accessor are in `ROLE_ROUTING_EXCLUDED`. `InflightTracker`'s direct
  AR access (`lookup_admitted_at`, the heartbeat thread) wraps
  explicitly. Staging tables and the adapter's table must live in the
  same DB for atomicity to hold.
- **`ManualAdmission.force!` (UI admit/drain) pre-inserts inflight rows**
  in the same TX as the claim, through the same
  `InflightTracker.pre_insert_admitted!` the Tick uses. Don't remove it:
  without it the concurrency gate under-counts force-admitted jobs until
  each one starts performing (over-admission window). It runs in the web
  process, whose registry is only populated as a side effect of job
  classes loading, so an unknown policy there means "we can't see it",
  NOT "it has no gate" — the helper inserts unless it knows there is no
  tracked gate, and `force!` warns. Erring the other way over-admits.
  It also charges the throttle, for the same reason: the button exists to
  bypass the gate's DECISION, not its COST, and a drain that leaves the
  bucket alone hands the tenant everything it forwarded plus an untouched
  window. Only a fixed `rate` and `per` can be charged here — a proc
  needs the partition's ctx, which this path never loads — so a dynamic
  throttle is left alone and warns.
- **Inflight rows are reaped on `perform.active_job` AND
  `discard.active_job`.** The railtie subscribes to both and calls
  `InflightTracker.handle_discard`, deleting the row by `active_job_id`.
  `discard` alone is NOT enough: ActiveJob instruments it in exactly one
  place, the rescue_from handler `discard_on` installs, so a job class
  with no handler dies in perform_now's bare `rescue Exception` and emits
  nothing. `perform.active_job` wraps the whole of perform_now — argument
  deserialization included — and carries an `:exception` payload when the
  job dies, which is what actually covers a job killed BEFORE
  around_perform, whose `ensure` never runs — otherwise the Tick's pre-inserted row sits until the
  `inflight_queued_stale_after` sweeper (1h), holding a slot.
- **Adding a table?** Add it to `Repository::ALL_TABLES` — the test
  bootstrap (`PostgresTest`) and the benchmark harness (`Bench`) both
  read that list to create, drop and truncate, so a table missing from
  it silently breaks schema rebuilds and leaks state between tests —
  AND update both the migration and the generator template, per the
  workflow below. Column added? Add it to
  `PostgresTest::SCHEMA_COLUMNS` too, UNDER ITS OWN TABLE KEY (the hash is
  keyed by table; a column filed under the wrong one can never be
  satisfied, so every integration test pays a full re-migrate); that's
  the drift check that
  rebuilds a stale local database.
- **Inflight tracking is decided from the POLICY at runtime, never
  from where the class was declared.** `Policy#inflight_tracked_gate`
  (`:concurrency` / `:adaptive_concurrency`, listed in
  `InflightTracker::TRACKED_GATES`) drives creation —
  `InflightTracker.pre_insert_admitted!`, called by both Tick and
  `ManualAdmission` — and `InflightTracker.track` reads the same fact
  to decide whether to release. **Including `InflightTracker` IS
  installing the callback** (its `included` block registers the
  `around_perform`), and `JobExtension` declares it as a Concern
  dependency, so anything that can be staged can be released. Do NOT
  reintroduce a per-class "installed" flag or install the callback
  from the `dispatch_policy` macro: that made creation policy-driven
  and release macro-driven, and a class bound with
  `dispatch_policy_name = "x"` — public API, and the only way to share
  one policy across classes — got rows nothing ever deleted, wedging
  the partition at `max` for an hour at a time. The key is always the
  partition ROW's `partition_key` — read it, never recompute it from ctx.
  That is a GATE-side rule: gates always have the partition row. At
  perform time there is none, so the key comes from the inflight row the
  Tick pre-inserted (`InflightTracker.lookup_admission`), which is the
  admission's own record of what it decided. One caller still recomputes
  — `InflightTracker.track`'s own `insert_inflight!` — and that is inert
  by `ON CONFLICT DO NOTHING` while the Tick's row exists. Closing it for
  real means stamping the admitted key into the forwarded payload, a
  format change larger than the exposure (a >1h queue wait plus an edit
  to `partition_by`) justifies. An adaptive observation keyed on a
  recomputed value files the AIMD state where `evaluate` will never look. `policy.partition_for(ctx)` returns the same value only while
  nobody edits `partition_by`; the moment somebody does, a gate counting
  under the recomputed key stops seeing the rows the admission path
  wrote under the stored one, and the cap silently lapses for every
  partition that predates the edit. "By construction the same value" was
  the wording here for three audits and it is what made that invisible. `dispatch_policy_inflight_tracking` only sets a flag that ADDS
  tracking for a policy WITHOUT such a gate (a live in-flight count on
  the dashboard); it installs nothing.
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
- **`:adaptive_concurrency` composes with fairness**. The two
  layers don't share state: fairness writes
  `partitions.decayed_admits` (in the admit TX), adaptive writes
  `dispatch_policy_adaptive_concurrency_stats.current_max` (in the
  worker's around_perform). Tables and locks are distinct. The
  per-partition admit_count is
  `min(fair_share, current_max - in_flight)` per tick, with the
  safety valve `max(remaining, initial_max)` when `in_flight=0`.
  Integration test:
  `test/integration/adaptive_with_fairness_test.rb`.
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
- **A staged row this process cannot deliver is HELD, not failed.** `Forwarder.dispatch` raises `UndeliverableJob` (carrying the
  staged ids) when the row cannot be deserialized, and both admission
  paths mark those rows `failed_at` outside the rolled-back transaction,
  decrement `pending_count`, and retry the admission ONCE. Without it the
  row sits at the head of its partition's claim order forever and the
  healthy rows behind it never leave — the claim is the only thing that
  deletes from `staged_jobs` and there is no retention sweep for it.
  Marked rather than deleted on purpose: dropping a staged job silently
  is exactly the at-least-once violation the admission TX exists to
  prevent. The inverse is `Repository.requeue_quarantined_jobs!` (the
  Requeue button), NOT clearing `failed_at` by hand — the quarantine
  decremented `pending_count` and `claim_partitions` needs it above zero,
  so a hand-cleared row is deliverable and unclaimable at the same time.
  Everything that reads staged rows has to agree with the claim's
  `failed_at IS NULL`: the scheduled park's due-work guard does, and the
  partition sweeper anti-joins against `staged_jobs` so it cannot collect
  a partition whose only remaining rows are quarantined.
  The hold EXPIRES — `TickLoop.sweep!` releases anything older than
  `config.quarantine_retry_after`. Do not make it terminal: the trigger
  is "this process cannot deserialize the row", and the ordinary cause is
  a rolling deploy whose tick pod is a release behind the web pods, which
  fixes itself minutes later. A terminal hold drops that class's whole
  backlog silently and for good, which is the at-least-once violation the
  admission TX exists to prevent. **Any** deserialize failure is held, not
  just an unresolvable constant: narrowing the rescue lets everything else
  escape to `Tick`'s generic rescue, which queues a backoff but writes no
  `failed_at`, so nothing ever releases the row and it heads every
  subsequent claim of that partition forever. The trigger is not exotic
  and is usually not a `NameError`: ANY error out of `klass.deserialize`
  does it — a `deserialize` override reading a field a pre-upgrade payload
  lacks (`KeyError`), one that touches the database (`RecordNotFound`), or
  a staged row whose `job_data` this gem did not write (a data migration,
  an import, a hand-edited row) whose `scheduled_at` is not an iso8601
  string (`TypeError` out of stock `deserialize_time`). Do not try to
  enumerate the classes — enumerating is what got this rescue narrowed
  twice. The invariant is "this process could not deserialize the row". Now that the hold expires, holding a transient failure
  for one `quarantine_retry_after` window beats wedging a partition
  permanently. `UnresolvableJobClass` stays a distinct class only so the
  log names the ordinary case — it is not a narrower rescue. Pinned by
  `test_a_deserialize_failure_that_is_not_a_name_error_is_held_too`; this
  rescue has been narrowed and reverted twice already.
- **Every multi-row writer of `partitions` takes its locks in
  `(policy_name, partition_key)` BYTE order.** Which collation is not a
  detail: `stage_many!` sorts in Ruby, i.e. `String#<=>`, i.e. bytes, so
  the SQL side must say `COLLATE "C"` and not merely `ORDER BY`. A bare
  `ORDER BY` inherits the database collation, and `en_US.UTF-8` — the
  default on RDS, Heroku, the official postgres image and Debian/Ubuntu —
  disagrees with byte order on ordinary keys (`acct:10` vs `acct:1:eu`,
  `acme` vs `Acme`, `user1` vs `user_1`). Two writers ordering by
  different collations is not ordering at all: measured at 18 deadlocks
  in 20s with a bare ORDER BY, 0 with `COLLATE "C"`. `stage_many!` sorts its groups
  for this reason and says so; `bulk_record_partition_denies!` now takes
  an explicit ordered `SELECT … FOR UPDATE` before its
  `UPDATE … FROM (VALUES …)`, because a single statement locks in HEAP
  order — not the order of its VALUES list, so sorting the Ruby array
  fixes nothing. Without it the two deadlock under an ordinary tick loop
  plus one `perform_all_later` process, and losing the flush loses every
  denied partition's backoff for that tick. If you add another statement
  that writes several partition rows, give it the same order. Two do not
  have it today: `sweep_inactive_partitions!`'s DELETE, safe only because
  it runs every `sweep_every_ticks`, and `PoliciesController#pause` /
  `#resume`, whose `update_all` covers every partition of the policy in
  index order. The second fires on an operator click, at the worst
  possible moment — during the load that made someone want to pause —
  and a deadlock there rolls the whole transaction back, so the policy is
  NOT paused, the tick keeps admitting, and the controller answers 500
  with nothing saying the pause failed. Measured at 5 deadlocks in 12
  clicks against one bulk-enqueuing process.
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
- **`.ruby-version` (3.2.2) is NOT the supported floor.** The gemspec
  still says `>= 3.1.0` and CI still runs the 3.1 leg — that matrix row
  exists precisely to prove the floor. Local dev moved off 3.1.4 because
  it does not build on current macOS: `ext/socket` fails silently, so
  `rbenv` reports success and you get a Ruby whose `require "socket"`
  raises, which takes `openssl`, bundler and `pg` down with it. Do not
  "align" the two by raising the gemspec floor or dropping the CI row —
  the cost of the split is that a 3.2-only idiom compiles locally and is
  caught only by CI, which is the right place for it to be caught.

## How to develop

```bash
# Start the dummy app (web + worker + tick) with foreman
bin/dummy setup good_job        # creates the DB and migrates
DUMMY_ADAPTER=good_job bundle exec foreman start

# Useful endpoints
http://localhost:3000/                       # forms to enqueue
http://localhost:3000/dispatch_policy        # dashboard

# Tests
bundle exec rake test                        # 262 runs / 621 assertions

# When you add a column or table:
#   1. Edit db/migrate/20260501000001_create_dispatch_policy_tables.rb
#   2. Edit lib/generators/.../create_dispatch_policy_tables.rb.tt
#   3. For the live dummy app, ALTER TABLE manually (no incremental
#      migrations because v0.1 ships a single migration)
#   4. Add the table to Repository::ALL_TABLES (the test bootstrap and
#      the benchmark harness both build their DDL from it); for a new
#      COLUMN, add it under its own TABLE KEY in
#      PostgresTest::SCHEMA_COLUMNS in
#      test/test_helper.rb, which is the drift check that rebuilds a
#      stale local database.
#   5. CHANGELOG: add/extend an "Upgrade notes" subsection under
#      Unreleased stating the schema change and the exact SQL/steps an
#      EXISTING install must run (the gem ships a single migration, so
#      upgraders don't get it via db:migrate). Release notes must always
#      flag migration changes — this is a hard convention.
```

## Useful debug queries

```sql
-- Distribution of partitions per policy/shard with pending and lifetime
SELECT policy_name, shard, status, count(*) AS partitions,
       sum(pending_count) AS pending, sum(total_admitted) AS lifetime
FROM dispatch_policy_partitions
GROUP BY policy_name, shard, status
ORDER BY pending DESC;

-- Partitions currently in backoff.
-- `tokens` is the balance AS OF `refilled_at`, and nothing rewrites it
-- between admissions (evaluate persists nothing), so read the raw column
-- and you will see a bucket frozen at the last admit. `live_tokens`
-- applies the refill the gate would apply — substitute the policy's own
-- rate/per for the 10/60 below, and its capacity for the LEAST bound.
SELECT policy_name, partition_key,
       (gate_state -> 'throttle' ->> 'tokens')::float AS tokens_at_last_admit,
       LEAST(
         (gate_state -> 'throttle' ->> 'tokens')::float
         + GREATEST(EXTRACT(EPOCH FROM now())
                    - (gate_state -> 'throttle' ->> 'refilled_at')::float, 0) * (10 / 60.0),
         10
       ) AS live_tokens,
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
