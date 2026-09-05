# Mutation battery

```bash
bundle exec rake mutations:list          # the catalogue, no work done
bundle exec rake mutations:check         # do the find-strings still match? (seconds)
bundle exec rake mutations:all           # 70 mutations, 69 must be caught (slow: one suite each)
FILTER=19 bundle exec rake mutations:all # one mutation
FILTER=forwarder bundle exec rake mutations:all
```

The count above goes stale the moment somebody adds an entry, so treat it
as a smell test rather than a contract: `mutations:list` is the answer.
What is NOT optional is `mutations:check` — editing a line an existing
mutation already breaks stales that entry silently, and a stale entry
proves nothing while reading exactly like a passing one. Four went stale
in one sitting on the branch that added this task.

**One process per database, and the runner now enforces it.** Every
integration case TRUNCATEs all six tables in `setup`, so two suites on one
database wipe each other's rows mid-test — and the symptom does not look
like concurrency at all. Measured while a battery was running and a plain
`rake test` was pointed at the same database:
`348 runs, 836 assertions, 18 failures`, concentrated in
ManualAdmissionTest, whose cases all assert on global state for one fixed
partition (`150 jobs against a bucket of 100 is a debt of 50` came back as
a credit of 6), plus two mutations scoring NO RESULT on the per-suite
timeout — because under contention the suite does not merely fail, it
HANGS: a TRUNCATE takes ACCESS EXCLUSIVE and queues behind another
process's `SELECT … FOR UPDATE`, and with a Ruby thread in the wait chain
Postgres's deadlock detector never breaks it.

I first wrote this paragraph blaming machine load, which was wrong and is
worth recording: a byte-for-byte clone of the "bad" database runs green in
13.6s, and two concurrent suites on ONE database reproduce the reported
failures verbatim. The state was innocent; the second process was not.
`Runner.run` refuses to start when anything else is connected to
`MUTATION_DB`, and the "control run is not usable" abort now says so
instead of telling you to fix a suite that is fine.

Two ways a mutation can look fine and mean nothing, both of which have
happened here:

- **CAUGHT alone, SURVIVED in a full run.** That is a test with a timing
  window, not a flaky runner. A test that COUNTED how many statements the
  heartbeat issued passed against a per-statement implementation, because
  the subscriber wakes the asserting thread on the first one. Assert
  something with no window.
- **NO RESULT.** Almost always a test that HANGS under its own mutation
  rather than failing: an unbounded `Queue#pop` waiting for a signal the
  broken code never sends. Bound every wait in a test that drives a
  thread, and make the timeout `flunk` with a message.

A timeout is not free, which is why the runner cleans up after one. It
kills the whole process GROUP — `bundle exec rake test` is three processes
deep and killing only the pid we hold leaves the grandchild running
(measured at 36 orphaned `rake_test_loader` processes after a session with
several timeouts) — and it terminates whatever is still connected to
`MUTATION_DB`, because a hanging test can be holding a SESSION-level
advisory lock and one NO RESULT would otherwise make every later mutation
score NO RESULT too.

Never reach for `pkill -9` on a pattern to clean up after a run. A pattern
that matches a Postgres BACKEND takes the whole local cluster into crash
recovery — SIGKILL on a backend makes the postmaster assume shared memory
is corrupt and reset every connection. That happened here. Use
`psql -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE datname = 'yours'"`, which asks the backend to exit cleanly and
release its locks.

Needs Postgres, like the integration suite. `MUTATION_DB` overrides the
database (do it if two people run at once — a shared one produces
failures that belong to neither run) and `MUTATION_TIMEOUT` the per-suite
bound. It copies the tree to a
temporary directory and works there, so an interrupted run cannot leave
your checkout broken; the database (`dispatch_policy_mutations`, override
with `MUTATION_DB`) is created if missing.

## What it is for

Not coverage. It answers one question, the one that reading a test
cannot: **would this test have failed before the fix?**

That question has a history here. The fourth audit's fix branch went
through five review rounds, and the first three each found the fixes
defective — never the audit. The recurring defect was always the same
shape: a test that passes against the bug it was written for. Four of
five did. Among them:

- a test that pinned the *bind order* of a lock statement rather than its
  SQL, so deleting the `ORDER BY` — the entire fix — left it green;
- a test that installed its own notification subscription, making the
  railtie wiring it claimed to cover invisible to it;
- a test that turned red when the **correct** fix was applied.

Each looked right. Each was written by someone who had just fixed the bug
and knew exactly what the code should do. That is precisely the state in
which you cannot judge your own test, and it is why this exists.

## The six outcomes

| Outcome | Meaning |
| --- | --- |
| `CAUGHT` | The suite failed. The line is guarded. |
| `SURVIVED` | The suite passed on broken code. Nothing guards it. |
| `NO TARGET` | The `find` string is gone from the file. The mutation is stale and proves **nothing**. |
| `INVALID` | The mutation produced a file that does not parse. Proves nothing — the suite never ran. |
| `NO RESULT` | The suite produced no summary line: it could not boot, bundler failed, the database was gone, it hung past `MUTATION_TIMEOUT`. Proves nothing either, and this is the one that hides — from the outside a non-green exit looks exactly like a catch. |
| `UNATTRIB` | The suite failed, but not in the test the entry names. Something is red; we cannot say this mutation is why. |

`CAUGHT` therefore requires two things: a parsed summary line saying what
failed, **and** the failure being in the test the entry claims should
notice. Without the second, a `CAUGHT` means only "something was red" —
a leaked `idle in transaction` backend on a shared database once made an
unrelated mutation fail 25 tests and score `CAUGHT`, and a stale
`caught_by` can point at a test that has not guarded that line in months.
The report prints the classes that actually failed, so the label is
checkable at a glance.

The last three fail the run, and they are not pedantry. The battery's own
mutation of the operator hint was mis-typed into a syntax error three
times running. The suite could not boot, the runner read "not green" as
"caught", and a line everybody believed was covered was not — the same
line that later 500'd the dashboard in production code. **A mutation that
did not actually run must never count as a pass** — which is also why
`NO RESULT` exists: the first version of this runner scored a suite that
never booted as `CAUGHT`, reproducing in the tool the exact defect the
tool is for.

`SURVIVED` is allowed only for entries in `EXPECTED_SURVIVORS`, each
carrying the argument for why it is unreachable. If one of those is ever
`CAUGHT`, the run says so: the note has gone stale and should be deleted.
Write that argument from the property that actually makes the mutation
inert — the first one written here named the wrong mechanism and was
plausible enough to survive a review.

## Source pins

Three entries (07, 22, 29) assert on a file's *source text* rather than
running it, because Rails does not boot in the test environment and the
lines are a railtie hook and a controller argument. Each says so in its
comment. A source pin passes for any change that leaves the asserted
characters somewhere in the file, so use one only when execution is
genuinely impossible, and pin the executable half separately — 28 runs
the predicate that 29 only checks is wired up. Two entries were source
pins with no such excuse: the dashboard tile and the hint's AND. Both are
now executed, via `DispatchPolicy::Overview`, which exists precisely so a
test can call them.

## Adding one

When you fix a defect, add the mutation that puts it back:

```ruby
{
  id:    "35",
  label: "short description of the breakage",
  # Why the line matters: the invariant, and what goes wrong without it.
  caught_by: "the_test_that_must_notice",
  file:  "lib/dispatch_policy/…",
  find:  "the exact source text",
  replace: "the broken version"
},
```

`find` is an exact substring match, single-quoted so `#{…}` in SQL
heredocs stays literal. Run `FILTER=<id>` and confirm `CAUGHT`. If it
survives, the test you just wrote is decorative — **fix the test, not the
catalogue.**

Four entries (19, 24, 30, 34) are the same `rescue` narrowed four
different ways. That is deliberate: it is the line this project has
actually broken and reverted twice, and a rescue that lists error classes
walks past a test that stages one concrete failure. Reverts of real past
mistakes are the most valuable entries here.
