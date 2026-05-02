# Benchmarks

Manual benchmark suite. Talks to a real Postgres, takes seconds to
minutes per run. Not part of `rake test`.

## Setup

```bash
createdb dispatch_policy_bench
DB_NAME=dispatch_policy_bench bundle exec rake bench
```

By default the suite uses the `dispatch_policy_bench` DB. Override with:

```
DB_HOST=...  DB_USER=...  DB_PASS=...  DB_NAME=...
```

## What's measured

| Script                  | What it measures                                                                  |
|-------------------------|-----------------------------------------------------------------------------------|
| `bench_tick.rb`         | `Tick.run` end-to-end at 100/1k/10k partitions, fairness overhead, tick budget    |
| `bench_stage.rb`        | `Repository.stage!` and `stage_many!` throughput                                  |
| `bench_claim.rb`        | `Repository.claim_partitions` SELECT … FOR UPDATE SKIP LOCKED at high cardinality |
| `bench_forwarder.rb`    | `Forwarder.dispatch` bulk vs scheduled path, against TestAdapter                  |
| `bench_real_adapter.rb` | Tick.run + Forwarder.dispatch against a real PG-backed adapter (good_job, solid_queue), including the actual INSERT into the adapter's table |

## Running

```bash
# All benchmarks (default DB = dispatch_policy_bench)
bundle exec rake bench

# Just one
bundle exec rake bench:tick

# Filter run_all.rb by name fragment
bundle exec ruby test/benchmark/run_all.rb tick

# More samples (default = 5 runs, first discarded as warmup)
RUNS=10 bundle exec rake bench

# Include 100k-partition scenarios in bench_claim (~slow)
BIG_SCALES=1 bundle exec rake bench:claim
```

Each script prints a Markdown table on stdout. Capture into a file:

```bash
bundle exec rake bench > bench-$(date +%F).md
```

## What numbers mean

The default benchmarks (`rake bench`) measure the **gem's** overhead in
isolation — they use the TestAdapter for `Forwarder.dispatch` so adapter
INSERT cost (good_job / solid_queue writing to their own table) is
excluded. For end-to-end production-shaped numbers (admission TX +
adapter INSERT) run the real-adapter bench:

```bash
# good_job, against the dummy DB (must already have good_jobs table)
bin/dummy setup good_job   # one-time
DUMMY_ADAPTER=good_job bundle exec rake bench:real

# solid_queue, against a separate DB
BENCH_ADAPTER=solid_queue BENCH_DB_NAME=dispatch_policy_solid_queue \
  bundle exec rake bench:real
```

The real-adapter bench reports a "Δ adapter cost" row that's the wall-
time difference between the gem-only run (TestAdapter) and the real
adapter run — that's the part that goes away if you turn the gem off.

⚠️ The real-adapter bench TRUNCATES the gem's tables AND the adapter's
jobs table on the bench DB. Stop the dummy foreman before running it.

The numbers are wall-clock medians across `RUNS` samples (default 5,
first discarded as warmup). They're not microbenchmarks — context
switches, GC, autovacuum, and OS noise will move things around. Run
twice if a number looks weird.
