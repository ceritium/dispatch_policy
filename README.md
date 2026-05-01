# dispatch_policy

Per-partition admission control for ActiveJob, on PostgreSQL.

`dispatch_policy` intercepts `perform_later` for jobs that declare a policy,
stages them in a dedicated table partitioned by a key you choose, and runs a
periodic *tick loop* that releases jobs through declared *gates* (currently
**throttle** and **concurrency**) into the real ActiveJob adapter
(`good_job`, `solid_queue`, …).

It is built for the case of millions of jobs queued against thousands of
"logical partitions" — e.g. one rate-limited HTTP endpoint per tenant, or a
concurrency cap per account — where existing adapters do not give you a fine
enough lever.

## Features

- Drop-in: declare a policy on any `ActiveJob` and `perform_later` works the
  same; the gem stages and forwards transparently.
- Two gates in v0.1: `throttle` (token-bucket) and `concurrency` (in-flight cap).
- One row per *live* partition, evaluated round-robin by `last_checked_at`
  for fairness.
- **Dynamic configuration is refreshed on every enqueue.** When the gate
  budget is computed from a DB attribute (`max: ->(ctx) { ctx[:max_per_account] }`),
  any change to that value is picked up by the very next admission as soon as
  one new job arrives for that partition.
- Compatible with `good_job` and `solid_queue` (any ActiveJob adapter on top
  of Postgres works in principle).
- `perform_later_all` (Rails 7.1+) goes through a single multi-row `INSERT`.
- Configurable retry strategy per policy (re-stage retries through gates, or
  bypass them and let the adapter retry directly).
- Embedded operation: a `DispatchTickLoopJob` you schedule like any other
  background job — no separate process to babysit.
- Rails engine UI for inspecting policies, partitions, and staged jobs.
- Supports `perform_in` / `set(wait:)`.

## Installation

Add to your `Gemfile`:

```ruby
gem "dispatch_policy"
```

Then install:

```bash
bundle install
bin/rails generate dispatch_policy:install
bin/rails db:migrate
```

The generator copies a migration, an initializer, and an
`app/jobs/dispatch_tick_loop_job.rb` template adapted to whichever adapter
you have configured.

Mount the engine in `config/routes.rb`:

```ruby
mount DispatchPolicy::Engine, at: "/dispatch_policy"
```

Schedule the tick job. Examples:

```ruby
# good_job: in config/initializers/good_job.rb
GoodJob.configure do |c|
  c.cron = {
    dispatch_tick_loop: { cron: "* * * * *", class: "DispatchTickLoopJob" }
  }
end

# solid_queue: in config/recurring.yml
production:
  dispatch_tick_loop:
    class: DispatchTickLoopJob
    schedule: every minute
```

The job self-chains every second, so the cron above is just a safety net to
restart the chain if a process is killed mid-loop.

## Declaring a policy

```ruby
class FetchEndpointJob < ApplicationJob
  dispatch_policy_inflight_tracking         # only required if a concurrency gate is used

  dispatch_policy :endpoints do
    context ->(args) {
      event = args.first
      {
        endpoint_id:     event.endpoint_id,
        rate_limit:      event.endpoint.rate_limit,
        account_id:      event.account_id,
        max_per_account: event.account.dispatch_concurrency
      }
    }

    gate :throttle,
         rate:         ->(ctx) { ctx[:rate_limit] },
         per:          1.minute,
         partition_by: ->(ctx) { "ep:#{ctx[:endpoint_id]}" }

    gate :concurrency,
         max:          ->(ctx) { ctx[:max_per_account] || 5 },
         partition_by: ->(ctx) { "acct:#{ctx[:account_id]}" }

    retry_strategy :restage      # default; alternative: :bypass
  end

  def perform(event)
    # ... call the rate-limited HTTP endpoint
  end
end
```

`perform_later` and `perform_later_all` work transparently:

```ruby
FetchEndpointJob.perform_later(event)
ActiveJob.perform_all_later(events.map { |e| FetchEndpointJob.new(e) })
```

### How the gates compose

Each gate declares a `partition_by` proc. The staged-job partition key is the
canonical concatenation of all gate partitions: `throttle=ep:42|concurrency=acct:7`.
This means a single `endpoint+account` combination has its own staged-jobs
queue, and the `concurrency` gate's partition (`acct:7`) is shared across all
staged partitions that map to the same account.

### `partitions.context` is refreshed on every enqueue

When you call `perform_later`, the gem evaluates your `context` proc and
upserts the partition row with the resulting hash:

```sql
INSERT INTO dispatch_policy_partitions (..., context, context_updated_at, ...) VALUES ($1, ...)
ON CONFLICT (policy_name, partition_key) DO UPDATE
  SET context            = EXCLUDED.context,
      context_updated_at = EXCLUDED.context_updated_at,
      pending_count      = dispatch_policy_partitions.pending_count + 1,
      ...
```

The tick loop then evaluates gate budgets against `partition.context`, **not**
against the per-job snapshot stored in `staged_jobs.context`. So if a tenant
bumps their `dispatch_concurrency` from 5 to 20 and a new job arrives, the
next admission will use the new value — no need to drain the partition first.

If a partition has no new traffic, the context stays at the value seen by the
last enqueue. (A TTL-based forced refresh is left as an extension hook.)

### Retry strategies

By default a retry produced by `retry_on` re-enters the policy and is staged
again, so throttle/concurrency apply equally to first attempts and retries.
For long-running retry storms where you want to favour throughput over
fairness, use `retry_strategy :bypass` to send retries straight to the
adapter.

## Sharding a single policy across worker pools

`shard_by` lets you split a single policy's partitions across N tick
loops, so admission scales horizontally. The shard is just a routing
label on each partition row; gate semantics are unchanged.

```ruby
class EventsJob < ApplicationJob
  # The job's queue is derived from the account so shard == queue.
  queue_as do
    attrs = arguments.first || {}
    "events-shard-#{attrs["account_id"].to_s.hash.abs % 4}"
  end

  dispatch_policy :events do
    context ->(args) { { account_id: args.first["account_id"] } }
    shard_by ->(ctx) { ctx[:queue_name] }     # use the queue itself

    gate :concurrency,
         max:          50,
         partition_by: ->(c) { "acct:#{c[:account_id]}" }
  end
end
```

Operator deploys one `DispatchTickLoopJob` per shard:

```ruby
4.times { |i| DispatchTickLoopJob.perform_later("events", "events-shard-#{i}") }
```

The generated `DispatchTickLoopJob` template uses `queue_as { arguments[1] }`
so the tick job is enqueued onto the same queue it monitors. Workers
listening on `events-shard-*` queues run both the tick loops and the
admitted jobs from one pool per shard.

The gem's automatic context enrichment puts `:queue_name` into the ctx
hash so `shard_by` can use it directly without your `context` proc
having to know about it.

### Backward compatibility

`shard_by` is opt-in. Without it every partition lives on the
`"default"` shard and `DispatchTickLoopJob.perform_later("events")`
(no shard argument) processes every partition — exactly like before.

### Don't shard finer than your throttle

A throttle bucket lives on the partition row. If two partitions sharing
the same `partition_by` for `:throttle` end up on different shards, each
shard runs its own bucket and the effective rate becomes
`rate × shards`. As a rule, `shard_by` should be at least as coarse as
the most restrictive `partition_by` of any rate-limiting gate. The
canonical safe choice is `shard_by ->(c) { c[:queue_name] }` plus a
`queue_as` that's a function of the throttle's partition_by.

## The tick loop

`DispatchPolicy::TickLoop.run(policy_name:, stop_when:)` claims partitions
under `FOR UPDATE SKIP LOCKED`, evaluates gates against each partition's
fresh context, atomically moves admitted rows from `staged_jobs` into the
real adapter (with a compensation that puts them back on enqueue failure),
and updates `partitions.gate_state` and `next_eligible_at`.

Run it embedded in your worker via the generated `DispatchTickLoopJob`. To
shard the system across workers, run **one job per `policy_name`**:

```ruby
DispatchTickLoopJob.perform_later("endpoints")
DispatchTickLoopJob.perform_later("notifications")
```

Each job uses `good_job_control_concurrency_with` (or
`limits_concurrency`) so only one tick is active per policy at a time.

## Configuration

```ruby
# config/initializers/dispatch_policy.rb
DispatchPolicy.configure do |c|
  c.tick_max_duration         = 25       # seconds the tick job stays admitting
  c.partition_batch_size      = 50       # partitions claimed per tick iteration
  c.admission_batch_size      = 100      # max jobs admitted per partition per iteration
  c.idle_pause                = 0.5      # seconds slept when a tick admits nothing
  c.partition_inactive_after  = 86_400   # GC partitions idle this long
  c.inflight_stale_after      = 300      # GC inflight rows whose worker stopped heartbeating
  c.sweep_every_ticks         = 50       # how often to run sweepers
end
```

You can override `admission_batch_size` per policy via the DSL.

## UI

Mount the engine and visit `/dispatch_policy`:

- **Dashboard** — totals, per-policy counters.
- **Policies** — pause/resume per policy.
- **Partitions** — searchable list, with detail view: gate state, recent
  staged jobs, force-admit and clear actions.

The UI auto-refreshes via `<turbo-frame refresh="morph">` (when `turbo-rails`
is on the host) plus a meta refresh fallback. CSRF and protection from
forgery use the host app's settings.

The UI ships unauthenticated, like the Sidekiq dashboard. Wrap the `mount`
with a constraint or `before_action` for auth in production.

## Compatibility

- Rails 7.1+ (tested on Rails 8.1).
- PostgreSQL 12+ (uses `FOR UPDATE SKIP LOCKED` and JSONB).
- `good_job` (>= 4.0) or `solid_queue` (>= 1.0).
- Sidekiq is **not** supported in v0.1 (the gem assumes Postgres for both
  staging and the real adapter).

## Dummy app

This repo ships with a dummy Rails app for hacking on the gem against either
adapter:

```bash
bin/dummy setup good_job        # create test DB, run migrations
bin/dummy good_job              # boot web + worker + tick loop with foreman
# or for solid_queue:
bin/dummy setup solid_queue
bin/dummy solid_queue
```

Then visit `http://localhost:3000` to enqueue jobs and
`http://localhost:3000/dispatch_policy` for the UI.

## Tests

```bash
DB_NAME=dispatch_policy_test bundle exec rake test
```

Unit tests run without a database; integration tests skip if Postgres is not
reachable.

## Status

v0.1 — the basics. Roadmap:

- `fair_interleave` gate (explicit weights between partitions).
- `global_cap` gate (policy-wide in-flight cap).
- `adaptive_concurrency` gate (signal-driven AIMD).
- Mission-Control-style metrics export.
- Multi-schema multi-tenant support.

## License

MIT.
