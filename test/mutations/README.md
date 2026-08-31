# Mutation battery

```bash
bundle exec rake mutations:list          # the catalogue, no work done
bundle exec rake mutations:all           # break each line in turn (slow: one suite per mutation)
FILTER=19 bundle exec rake mutations:all # one mutation
FILTER=forwarder bundle exec rake mutations:all
```

Needs Postgres, like the integration suite. It copies the tree to a
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

## The five outcomes

| Outcome | Meaning |
| --- | --- |
| `CAUGHT` | The suite failed. The line is guarded. |
| `SURVIVED` | The suite passed on broken code. Nothing guards it. |
| `NO TARGET` | The `find` string is gone from the file. The mutation is stale and proves **nothing**. |
| `INVALID` | The mutation produced a file that does not parse. Proves nothing — the suite never ran. |
| `NO RESULT` | The suite produced no summary line: it could not boot, bundler failed, the database was gone. Proves nothing either, and this is the one that hides — from the outside a non-green exit looks exactly like a catch. |

`CAUGHT` therefore requires a parsed summary line saying what failed, not
merely a non-zero exit.

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
