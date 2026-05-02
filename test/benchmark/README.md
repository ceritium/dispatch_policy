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

| Script                | What it measures                                                   |
|-----------------------|--------------------------------------------------------------------|
| `bench_tick.rb`       | `Tick.run` end-to-end at 100/1k/10k partitions, fairness overhead, tick budget |
| `bench_stage.rb`      | `Repository.stage!` and `stage_many!` throughput                   |
| `bench_claim.rb`      | `Repository.claim_partitions` SELECT … FOR UPDATE SKIP LOCKED at high cardinality |
| `bench_forwarder.rb`  | `Forwarder.dispatch` bulk vs scheduled path, against TestAdapter   |

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

These benchmarks measure the **gem's** overhead — they use the
TestAdapter for `Forwarder.dispatch` so adapter INSERT cost (good_job /
solid_queue writing to their own table) is excluded. For end-to-end
production-shaped numbers (admission TX + adapter INSERT) you need a
separate harness against a real adapter, ideally pointed at the
adapter's actual prod-shaped table.

The numbers are wall-clock medians across `RUNS` samples (default 5,
first discarded as warmup). They're not microbenchmarks — context
switches, GC, autovacuum, and OS noise will move things around. Run
twice if a number looks weird.
