# DispatchPolicy

Per-partition admission control for ActiveJob. Stages `perform_later` into a
dedicated table, runs a tick loop that admits jobs through declared gates
(throttle, concurrency, global cap, fair interleave), then forwards survivors
to the real adapter.

Use it when you need:

- **Per-tenant / per-endpoint throttle** that's exact (token bucket) instead of
  best-effort enqueue-side.
- **Per-partition concurrency** with a proper release hook on job completion
  (and lease-expiry recovery if the worker dies mid-perform).
- **Dedupe** against a partial unique index, not an in-memory key.
- **Round-robin fairness across tenants** (LATERAL batch fetch) so one tenant's
  burst can't starve the others.

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

## Declaring a policy

```ruby
class SendWebhookJob < ApplicationJob
  include DispatchPolicy::Dispatchable

  dispatch_policy do
    context ->(args) {
      event = args.first
      { endpoint_id: event.endpoint_id, rate_limit: event.endpoint.rate_limit }
    }

    dedupe_key ->(args) { "event:#{args.first.id}" }

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

## Running the tick

The gem exposes `DispatchPolicy::TickLoop.run(policy_name:, stop_when:)` but
**does not ship a tick job** — concurrency semantics are queue-adapter
specific (GoodJob's `total_limit`, Sidekiq Enterprise uniqueness, etc.), so
you write a small job in your app that wraps the loop with whatever dedup
your adapter provides. Example for GoodJob:

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

Schedule it (every 10s as a safety net — the self-chain keeps one alive
under normal operation):

```ruby
# config/application.rb
config.good_job.cron = {
  dispatch_tick_loop: {
    cron:  "*/10 * * * * *",
    class: "DispatchTickLoopJob"
  }
}
```

For adapters without a first-class dedup mechanism, implement it yourself
(e.g. `pg_try_advisory_lock` inside `perform`) before calling
`DispatchPolicy::TickLoop.run`.

## Gates shipped

- `:concurrency` — max in-flight per partition.
- `:throttle` — token bucket, rate per partition per period.
- `:global_cap` — max in-flight across the whole policy.
- `:fair_interleave` — round-robin partition order within a batch.

## Testing

```
bundle install
bundle exec rake test
```

Tests require a PostgreSQL instance (uses `ON CONFLICT`, partial indexes,
`FOR UPDATE SKIP LOCKED`, `jsonb`). `PGUSER`/`PGHOST`/`PGPASSWORD` env vars
override the defaults in `test/dummy/config/database.yml`.

## License

MIT.
