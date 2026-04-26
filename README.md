# DispatchPolicy

> **⚠️ Experimental.** The API, schema, and defaults can change between
> minor releases without notice. DispatchPolicy is currently running in
> production on [pulso.run](https://pulso.run) — that's how we learn
> what breaks. If you pick it up for your own project, pin the exact
> version and expect to follow the changelog.
>
> **PostgreSQL only (11+).** The staging, admission, and fairness
> machinery lean on `jsonb`, partial indexes, `FOR UPDATE SKIP LOCKED`,
> `ON CONFLICT`, and `CROSS JOIN LATERAL`. MySQL/SQLite support isn't
> closed off as a goal — being drop-in across every ActiveJob backend
> is the long-term direction — but it would take meaningful rework
> (shadow columns for `jsonb`, full indexes instead of partial, a
> different batch-fetch strategy for fairness). Contributions welcome.

Per-partition admission control for ActiveJob. Stages `perform_later`
into a dedicated table, runs a tick loop that admits jobs through
declared gates (throttle, concurrency, global_cap, fair_interleave,
adaptive_concurrency), then forwards survivors to the real adapter.

Use it when you need:

- **Per-tenant / per-endpoint throttle** that's exact (token bucket)
  instead of best-effort enqueue-side.
- **Per-partition concurrency** with a proper release hook on job
  completion (and lease-expiry recovery if the worker dies mid-perform).
- **Adaptive concurrency** — a cap that shrinks under queue pressure
  and grows back when workers keep up, without manual tuning.
- **Dedupe** against a partial unique index, not an in-memory key.
- **Round-robin fairness across tenants** (LATERAL batch fetch) so one
  tenant's burst can't starve the others — including a **time-weighted
  variant** that balances total compute time per tenant when their
  performs have very different durations.

## Demo

A runnable playground that exercises every gate and the admin UI lives
at [ceritium/dispatch_policy-demo](https://github.com/ceritium/dispatch_policy-demo).
Clone it, `bundle && rails db:setup`, and use the in-browser forms to
fire jobs through throttle / concurrency / adaptive / round-robin
policies while the admin UI updates in real time.

## Install

Add to your `Gemfile`:

```ruby
gem "dispatch_policy"
```

Copy the migration and run it:

```
bundle exec rails dispatch_policy:install:migrations
bundle exec rails db:migrate
```

Mount the admin UI in `config/routes.rb` (optional):

```ruby
mount DispatchPolicy::Engine => "/admin/dispatch_policy"
```

Configure in `config/initializers/dispatch_policy.rb`:

```ruby
DispatchPolicy.configure do |c|
  c.enabled             = ENV.fetch("DISPATCH_POLICY_ENABLED", "true") != "false"
  c.lease_duration      = 15.minutes
  c.batch_size          = 500
  c.round_robin_quantum = 50
  c.tick_sleep          = 1        # idle
  c.tick_sleep_busy     = 0.05     # after productive ticks
end
```

## Flow

```
ActiveJob#perform_later
  → Dispatchable#enqueue
    → StagedJob.stage!   (insert into dispatch_policy_staged_jobs, pending)

(tick loop, periodically)
  → SELECT pending FOR UPDATE SKIP LOCKED
  → Run gates in declared order; survivors are the admitted set
  → StagedJob#mark_admitted!   (increment counters, set admitted_at)
  → job.enqueue(_bypass_staging: true)   (hand off to the real adapter)

(worker runs perform)
  → Dispatchable#around_perform
    → block.call
    → release counters, mark StagedJob completed_at, record observation
```

## Declaring a policy

```ruby
class SendWebhookJob < ApplicationJob
  include DispatchPolicy::Dispatchable

  dispatch_policy do
    # Persisted in the staged row so gates can read it without touching AR.
    context ->(args) {
      event = args.first
      { endpoint_id: event.endpoint_id, rate_limit: event.endpoint.rate_limit }
    }

    # Partial unique index dedupes identical keys while the previous is pending.
    dedupe_key ->(args) { "event:#{args.first.id}" }

    # Tenant fairness — see the "Round-robin" section below.
    round_robin_by ->(args) { args.first.account_id }

    gate :throttle,
         rate:         ->(ctx) { ctx[:rate_limit] },
         per:          1.minute,
         partition_by: ->(ctx) { ctx[:endpoint_id] }

    gate :fair_interleave
  end

  def perform(event) = event.deliver!
end
```

`perform_later` stages the job; the tick admits it when its gates pass.

For the common multi-tenant webhook case (mixed-latency tenants behind
a shared pool) skip ahead to [Recipes](#multi-tenant-webhook-delivery)
— `round_robin_by weight: :time` plus `:adaptive_concurrency` covers
it without an explicit throttle.

## Gates

Gates run in declared order, each narrowing the survivor set. Any option
that takes a value can alternatively take a lambda that receives the
`ctx` hash, so parameters can depend on per-job data.

### `:concurrency` — in-flight cap per partition

Caps the number of admitted-but-not-yet-completed jobs in each
partition. Tracks in-flight counts in
`dispatch_policy_partition_counts`; decremented by the `around_perform`
hook when the job finishes, or by the reaper when a lease expires
(worker crashed).

```ruby
gate :concurrency,
     max:          ->(ctx) { ctx[:max_per_account] || 5 },
     partition_by: ->(ctx) { "acct:#{ctx[:account_id]}" }
```

When to reach for it: external APIs with per-tenant concurrency limits,
database-heavy jobs you don't want to pile up per customer, anything
where "at most N running at once for this key" matters.

### `:throttle` — token-bucket rate limit per partition

Refills `rate` tokens every `per` seconds, capped at `burst` (defaults
to `rate`). Admits jobs while tokens are available; leaves the rest
pending for the next tick.

```ruby
gate :throttle,
     rate:         100,          # tokens
     per:          1.minute,     # refill window
     burst:        100,          # bucket cap (optional, defaults to rate)
     partition_by: ->(ctx) { "host:#{ctx[:host]}" }
```

`rate` and `burst` accept lambdas, so the limit can come from
configuration stored alongside the thing being rate-limited:

```ruby
gate :throttle,
     rate:         ->(ctx) { ctx[:rate_limit] },
     per:          1.minute,
     partition_by: ->(ctx) { ctx[:endpoint_id] }
```

Unlike `:concurrency`, throttle does **not** release tokens on job
completion — tokens refill only with elapsed time.

### `:global_cap` — single cap across all partitions

A global version of `:concurrency`: at most `max` jobs admitted
simultaneously across the whole policy, regardless of partition.
Useful as a safety ceiling on top of per-partition limits.

```ruby
gate :concurrency, max: 10, partition_by: ->(ctx) { ctx[:tenant] }
gate :global_cap,  max: 200
```

Reads: "up to 10 in flight per tenant, but never more than 200 total".

### `:fair_interleave` — round-robin ordering across partitions

Not a filter — a reordering step. Groups the batch by its primary
partition and interleaves, so no single partition can starve others
even if it has many pending jobs.

```ruby
gate :concurrency, max: 10, partition_by: ->(ctx) { "acct:#{ctx[:account_id]}" }
gate :fair_interleave
```

Place it after a gate that assigned partitions; interleaving is keyed
off the first partition a row picked up.

### `:adaptive_concurrency` — per-partition cap that self-tunes

The cap per partition (`current_max`) shrinks when the adapter queue
backs up (EWMA of queue lag > `target_lag_ms`) or when performs raise;
grows back by +1 when lag stays under target. AIMD loop on a
per-partition stats row (`dispatch_policy_adaptive_concurrency_stats`).

```ruby
gate :adaptive_concurrency,
     partition_by:   ->(ctx) { ctx[:account_id] },
     initial_max:    3,
     target_lag_ms:  1000,   # acceptable queue wait before admission
     min:            1       # floor so a partition can't lock out
end
```

- **Feedback signal**: `admitted_at → perform_start` (queue wait in the
  real adapter). Pure saturation signal — slow performs in the
  downstream service don't punish admissions if workers still drain
  the queue quickly.
- **Growth**: +1 per fast success. No hard ceiling; the algorithm
  self-limits via `target_lag_ms`. If the queue builds up, the cap
  shrinks multiplicatively.
- **Failure**: `current_max *= 0.5` (halve) when `perform` raises.
- **Slow**: `current_max *= 0.95` when EWMA lag > target.

### Choosing `target_lag_ms`

It's the knob that trades latency for throughput. Rough guide:

- **Too low** (e.g. 10-50 ms). The gate reacts to every tiny bump in
  queue wait and shrinks the cap aggressively. Workers can end up
  idle with jobs still pending admission because the cap is
  overcorrecting — classic contention / overshoot.
- **Too high** (e.g. 30 s). The gate barely ever pushes back, so
  you get near-maximum throughput at the cost of real queue buildup;
  newly admitted jobs may wait seconds before a worker picks them
  up.
- **Reasonable starting point**: `≈ worker_max_threads × avg_perform_ms`.
  If you run 5 workers at ~200 ms/perform, `target_lag_ms: 1000`
  means "it's OK if the adapter queue stays at most ~1 second
  deep". You'll want to tune from there based on what your
  downstream tolerates and how fast you want bursts to drain.

Pair it with `round_robin_by` for multi-tenant systems that want
automatic backpressure without hand-tuned caps per tenant:

```ruby
round_robin_by ->(args) { args.first[:account_id] }
gate :adaptive_concurrency,
     partition_by:  ->(ctx) { ctx[:account_id] },
     initial_max:   3,
     target_lag_ms: 1000
```

## Queues and partitioning

DispatchPolicy operates at the **policy** (class) level. A job's
ActiveJob `queue` and `priority` travel through staging into admission
and on to the real adapter — workers of each queue pick up their jobs
normally — but neither affects which staged rows the gates see. All
enqueues of the same job class share one policy, one throttle bucket,
one concurrency cap.

Two consequences to be aware of:

- Enqueuing the same job to different queues does **not** give one
  queue priority at admission; they share the policy's gates. If
  urgent work should jump ahead, set a lower ActiveJob `priority`
  (the admission SELECT is `ORDER BY priority, staged_at`) — or split
  into a subclass with its own policy.
- `dedupe_key` is queue-agnostic: the same key enqueued to
  `:urgent` and `:low` dedupes to one row.

### Using queue as a partition

The context hash has `queue_name` and `priority` injected automatically
at stage time (user-supplied keys win). Use them in any `partition_by`:

```ruby
class SendEmailJob < ApplicationJob
  include DispatchPolicy::Dispatchable

  dispatch_policy do
    context ->(args) { { account_id: args.first.account_id } }

    # Separate throttle bucket per (queue, account) — urgent and default
    # don't share rate tokens.
    gate :throttle,
         rate:         100,
         per:          1.minute,
         partition_by: ->(ctx) { "#{ctx[:queue_name]}:#{ctx[:account_id]}" }
  end
end

SendEmailJob.set(queue: :urgent).perform_later(user)
SendEmailJob.set(queue: :default).perform_later(user)
# → two partitions, each with its own bucket.
```

If you'd rather keep the two streams fully isolated (separate policies,
admin rows, and dedupe scopes), subclass:

```ruby
class UrgentEmailJob < SendEmailJob
  queue_as :urgent
  dispatch_policy do
    context ->(args) { { account_id: args.first.account_id } }
    gate :throttle, rate: 500, per: 1.minute, partition_by: ->(ctx) { ctx[:account_id] }
  end
end
```

## Dedupe

`dedupe_key` is enforced by a partial unique index on
`(policy_name, dedupe_key) WHERE completed_at IS NULL`. Semantics:

- Re-enqueuing while a previous staged row is pending or admitted →
  silently dropped.
- Re-enqueuing after the previous completes → fresh staged row.
- Returning `nil` from the lambda → no dedup for that enqueue.

Typical pattern: `"<domain>:<entity>:<id>"` (`"monitor:42"`,
`"event:abc123"`). Keep it stable for the duration of a logical unit
of work.

## Round-robin batching (tenant fairness)

For policies where every tenant should keep making progress even
when one suddenly enqueues 100× its normal volume, neither throttle
nor concurrency is a good fit — you want max throughput, just
fairness. `round_robin_by` solves it at the batch SELECT layer:

```ruby
dispatch_policy do
  context ->(args) { { account_id: args.first.account_id } }
  round_robin_by ->(args) { args.first.account_id }
end
```

At stage time the lambda's result is written into the dedicated
`round_robin_key` column (indexed). `Tick.run` then uses a two-phase
fetch:

1. **LATERAL join** — distinct keys × per-key `LIMIT round_robin_quantum`.
   Guarantees each active tenant gets at least `quantum` rows per
   tick, so a tenant with 10 pending is served in the same tick as
   a tenant with 50k pending.
2. **Top-up** — if the fairness floor doesn't fill `batch_size`, the
   remaining slots go to the oldest pending (excluding the ids
   already locked). Keeps single-tenant throughput at full capacity.

Cost per tick is O(`quantum × active_keys`), not O(backlog) — so the
admin stays snappy even with thousands of distinct tenants.

### Time-weighted variant

Equal-quanta round-robin gives every active tenant the same number of
admissions per tick — fair by *count*. If your tenants have very
different per-job durations (slow webhooks, varied report sizes) and
you want to balance the *total compute time* each consumes, pass
`weight: :time`:

```ruby
round_robin_by ->(args) { args.first[:account_id] }, weight: :time
```

Solo tenants are unaffected — the fetch falls through to the trailing
top-up and they consume up to `batch_size` per tick. When multiple
tenants are active, each one's quantum is sized inversely to how much
compute time it has used in the last `window` seconds (default 60),
sourced from `dispatch_policy_partition_observations`. So if `slow`
has burned 20 s of perform time recently and `fast` has burned 200 ms,
this tick `fast` claims ~99% of `batch_size` while `slow` gets the
floor — total compute per minute stays balanced and you don't need a
throttle on top.

## Recipes

### Multi-tenant webhook delivery

Mixed-latency tenants behind a shared worker pool — exactly the case
that motivated `weight: :time` and adaptive concurrency. Pair them:

```ruby
class WebhookDeliveryJob < ApplicationJob
  include DispatchPolicy::Dispatchable

  dispatch_policy do
    context ->(args) { { account_id: args.first[:account_id] } }

    # Fetch-level fairness by *compute time* (not request count). When
    # several accounts compete, per-tick quanta are sized inverse to
    # their recent perform duration; solo accounts top up to batch_size.
    round_robin_by ->(args) { args.first[:account_id] },
                   weight: :time, window: 60

    # Drip-feed admission per account based on adapter queue lag.
    # Without this, a single account with thousands of pending could
    # dump batch_size jobs into the adapter queue in one tick and lose
    # the ability to react to performance changes mid-burst.
    gate :adaptive_concurrency,
         partition_by:  ->(ctx) { ctx[:account_id] },
         initial_max:   3,
         target_lag_ms: 500
  end

  def perform(account_id:, **) = WebhookClient.deliver!(account_id)
end
```

What you get with no throttle, no manual tuning:

- A solo account runs at whatever throughput its downstream allows;
  `:adaptive_concurrency` grows `current_max` while queue lag stays
  under `target_lag_ms`.
- A slow account (1 s/perform) and a fast account (100 ms/perform)
  competing → `weight: :time` gives the fast one most of each tick's
  budget; the slow one's adaptive cap shrinks toward `min`. Total
  compute time per minute stays balanced and the adapter queue
  doesn't pile up behind whichever tenant happened to enqueue first.
- A misbehaving downstream that suddenly goes from 100 ms to 5 s →
  that tenant's `current_max` drops within a few completions and its
  fetch quantum shrinks; the other tenants are unaffected.

Tune `target_lag_ms` for the latency budget you can tolerate (see
[Choosing target_lag_ms](#choosing-target_lag_ms)) and `window` for
how reactive the time-balancing should be (smaller = noisier, larger
= more stable).

## Running the tick

The gem exposes `DispatchPolicy::TickLoop.run(policy_name:, stop_when:)`
but **does not ship a tick job** — concurrency semantics are
queue-adapter specific (GoodJob's `total_limit`, Sidekiq Enterprise
uniqueness, etc.), so you write a small job in your app that wraps
the loop with whatever dedup your adapter provides. Example for
GoodJob:

```ruby
# app/jobs/dispatch_tick_loop_job.rb
class DispatchTickLoopJob < ApplicationJob
  include GoodJob::ActiveJobExtensions::Concurrency
  good_job_control_concurrency_with(
    total_limit: 1,
    key: -> { "dispatch_tick_loop:#{arguments.first || 'all'}" }
  )

  def perform(policy_name = nil)
    deadline = Time.current + DispatchPolicy.config.tick_max_duration
    DispatchPolicy::TickLoop.run(
      policy_name: policy_name,
      stop_when:   -> {
        GoodJob.current_thread_shutting_down? || Time.current >= deadline
      }
    )
    # Self-chain so the next run starts immediately; cron below is a safety net.
    DispatchTickLoopJob.set(wait: 1.second).perform_later(policy_name)
  end
end
```

Schedule it (every 10s as a safety net — the self-chain keeps one
alive under normal operation):

```ruby
# config/application.rb
config.good_job.cron = {
  dispatch_tick_loop: {
    cron:  "*/10 * * * * *",
    class: "DispatchTickLoopJob"
  }
}
```

For adapters without a first-class dedup mechanism, implement it
yourself (e.g. `pg_try_advisory_lock` inside `perform`) before calling
`DispatchPolicy::TickLoop.run`.

## Admin UI

`DispatchPolicy::Engine` ships a read-only admin mounted wherever
you like. Features:

- Policy index with pending / admitted / completed-24h totals.
- Per-policy page with a **partition breakdown** (watched + searchable
  list) showing pending-eligible / pending-scheduled / in-flight /
  completed / adaptive cap / EWMA latency / last enqueue / last
  dispatch per partition.
- Line chart of avg EWMA queue lag (last hour, per minute) with
  completions-per-minute bars behind it.
- Per-partition sparkline with the same overlay; click to watch /
  unwatch. Watched set is persisted in `localStorage` and synced into
  the URL so reloading keeps your view.
- Opt-in auto-refresh (off / 2s / 5s / 15s) stored in `localStorage`.
  Page updates via Turbo morph — scroll position and tooltips survive.

## Testing

```
bundle install
bundle exec rake test
```

Tests require a PostgreSQL instance (uses `ON CONFLICT`, partial
indexes, `FOR UPDATE SKIP LOCKED`, `jsonb`). `PGUSER` / `PGHOST` /
`PGPASSWORD` env vars override the defaults in
`test/dummy/config/database.yml`.

## Releasing

The gem uses the standard `bundler/gem_tasks` flow — there is no
release automation in CI. To cut a new version:

1. Bump `DispatchPolicy::VERSION` in `lib/dispatch_policy/version.rb`
   following SemVer. While the API is marked experimental, breaking
   changes go in a minor bump and should be called out in the changelog.
2. Add a section to `CHANGELOG.md` above the previous one, grouping
   entries (Added / Changed / Fixed / Removed). Link any relevant PRs.
3. Make sure the working tree is on `master`, clean, and CI is green
   (`bundle exec rake test` locally for a sanity check).
4. Commit: `git commit -am "Release vX.Y.Z"`.
5. `bundle exec rake release` — Bundler will build the `.gem` into
   `pkg/`, tag `vX.Y.Z`, push the commit and tag, and `gem push` to
   RubyGems. The gemspec sets `rubygems_mfa_required`, so have your
   OTP ready (`gem signin` first if you aren't authenticated).
6. Optional: publish a GitHub release from the tag, e.g.
   `gh release create vX.Y.Z --notes-from-tag`, or paste the
   changelog section into the release notes.

If `rake release` fails partway through (e.g. RubyGems push rejects
the version), do not retry blindly — inspect what already happened
(tag created? commit pushed?) and clean up before re-running, since
Bundler won't re-tag an existing version.

## License

MIT.
