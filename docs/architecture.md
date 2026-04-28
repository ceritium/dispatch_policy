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
