# frozen_string_literal: true

module DispatchPolicy
  # The mutation catalogue: deliberate breakages of load-bearing lines,
  # each paired with the test that must notice.
  #
  # Why this exists. Four review rounds on the fourth audit's fix branch
  # found the same defect over and over, and it was never in the audit —
  # it was in the fixes: **a test that passes against the bug it was
  # written for.** Four of five did. Reading a test cannot tell you
  # whether it would have failed before the fix; running it against the
  # broken code can, and that is all this is.
  #
  # So a mutation is not a coverage metric. Each one is a specific claim
  # — "if someone writes this, a test fails" — and the ones that matter
  # most are the reverts: the lines this project has already broken once
  # and would break again (19, 24, 30 and 34 are the same rescue,
  # narrowed four different ways, because it has been narrowed and
  # reverted twice for real).
  #
  # Adding one: when you fix a defect, add the mutation that puts it
  # back. If it survives, your test is decorative — fix the test, not the
  # catalogue. See test/mutations/README.md.
  module Mutations
    # Mutations that are expected to survive, with the reason. A survivor
    # NOT listed here fails the run; an entry here that gets caught is
    # reported too, because it means the note is stale.
    EXPECTED_SURVIVORS = {
      "04" => "Unreachable by construction, though NOT for the reason first " \
              "written here (that the quarantine zeroes pending_count: it " \
              "subtracts with a GREATEST floor, so a partition with other " \
              "pending rows is still claimed). The real property is that the " \
              "MIN only counts rows with `scheduled_at > now()`, and the claim " \
              "only ever returns rows that are already due — so a row cannot be " \
              "both held and still scheduled in the future. If any new path can " \
              "hold a future-scheduled row, delete this entry and write the test."
    }.freeze

    ALL = [
  {
    id:    '01',
    label: 'deny flush: COLLATE dropped',
    # Every multi-row writer of `partitions` locks in BYTE order. A bare ORDER BY
    # inherits the database collation, which disagrees with Ruby's `String#<=>` on
    # ordinary keys — measured at 18 deadlocks in 20s against one bulk enqueue.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"',
      '            FOR UPDATE',
      '          SQL',
      '          "lock_partitions_for_deny",'
    ].join("\n"),
    replace: [
      '            ORDER BY policy_name, partition_key',
      '            FOR UPDATE',
      '          SQL',
      '          "lock_partitions_for_deny",'
    ].join("\n")
  },
  {
    id:    '02',
    label: 'deny flush: ORDER BY deleted',
    # The deny flush must take its locks explicitly. One statement locks in HEAP
    # order, so sorting the Ruby array fixes nothing.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"',
      '            FOR UPDATE',
      '          SQL',
      '          "lock_partitions_for_deny",'
    ].join("\n"),
    replace: [
      '            FOR UPDATE',
      '          SQL',
      '          "lock_partitions_for_deny",'
    ].join("\n")
  },
  {
    id:    '03',
    label: 'scheduled park: due-work guard ignores failed_at',
    # The scheduled park's due-work guard has to agree with the claim's
    # `failed_at IS NULL`, or a held row reads as due work and the park never fires.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '                AND d.failed_at IS NULL' + "\n",
    replace: ''
  },
  {
    id:    '04',
    label: 'scheduled park: horizon MIN ignores failed_at',
    # The park horizon's MIN should skip held rows for the same reason.
    caught_by: 'none — see EXPECTED_SURVIVORS',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '                  AND s.failed_at IS NULL' + "\n",
    replace: ''
  },
  {
    id:    '05',
    label: 'partition sweeper: anti-join against staged_jobs removed',
    # The partition sweeper must not collect a partition whose only remaining rows
    # are held: that strands them with nothing pointing at them.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            AND NOT EXISTS (',
      '              SELECT 1 FROM #{STAGED_TABLE} s',
      '              WHERE s.policy_name = p.policy_name',
      '                AND s.partition_key = p.partition_key',
      '            )'
    ].join("\n"),
    replace: ''
  },
  {
    id:    '06',
    label: 'requeue: pending_count not restored',
    # Requeue restores `pending_count` in the same statement. Without it the row is
    # deliverable and unclaimable at once — the exact trap of clearing failed_at by hand.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '            SET pending_count = pending_count + (SELECT count(*) FROM requeued),' + "\n",
    replace: '            SET '
  },
  {
    id:    '07',
    label: 'railtie: engine models not routed',
    # The engine's own models must be routed to the configured connection, or the
    # documented separate-queue-DB install cannot admit a job.
    caught_by: 'connection_identity_test',
    file:  'lib/dispatch_policy/railtie.rb',
    find:  '      DispatchPolicy.route_models_to_configured_connection!' + "\n",
    replace: ''
  },
  {
    id:    '08',
    label: 'lookup_admission: partition key dropped',
    # The inflight key comes from the row the Tick pre-inserted — the admission's own
    # record of what it decided — never recomputed from ctx.
    caught_by: 'live_policy_edit_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '[row["queue_lag_ms"].to_i, row["partition_key"]]',
    replace: '[row["queue_lag_ms"].to_i, nil]'
  },
  {
    id:    '09',
    label: 'adaptive gate: partition key recomputed from ctx',
    # An adaptive observation keyed on a recomputed value files the AIMD state where
    # `evaluate` will never look.
    caught_by: 'live_policy_edit_test',
    file:  'lib/dispatch_policy/gates/adaptive_concurrency.rb',
    find:  '        key         = partition["partition_key"]',
    replace: '        key         = DispatchPolicy.registry.fetch(policy_name).partition_for(ctx)'
  },
  {
    id:    '10',
    label: 'concurrency gate: partition key recomputed from ctx',
    # A concurrency gate counting under a recomputed key stops seeing the rows the
    # admission path wrote, and the cap silently lapses for every older partition.
    caught_by: 'live_policy_edit_test',
    file:  'lib/dispatch_policy/gates/concurrency.rb',
    find:  '          partition_key: partition["partition_key"]',
    replace: '          partition_key: DispatchPolicy.registry.fetch(partition["policy_name"]).partition_for(ctx)'
  },
  {
    id:    '11',
    label: 'concurrency gate: Integer() instead of Float().floor',
    # A jsonb count comes back as a String; `Integer()` raises on "3.0" where
    # `Float().floor` does not.
    caught_by: 'live_policy_edit_test',
    file:  'lib/dispatch_policy/gates/concurrency.rb',
    find:  'value.nil? ? 0 : Float(value).floor',
    replace: 'value.nil? ? 0 : Integer(value)'
  },
  {
    id:    '12',
    label: 'upsert: shard pinned unconditionally',
    # The shard is pinned while the partition holds work and recomputed once drained.
    # Pinning it unconditionally strands every pre-existing partition.
    caught_by: 'shard_change_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            shard               = CASE',
      '              WHEN #{PARTITIONS_TABLE}.pending_count = 0 THEN EXCLUDED.shard',
      '              ELSE #{PARTITIONS_TABLE}.shard',
      '            END,'
    ].join("\n"),
    replace: '            shard               = #{PARTITIONS_TABLE}.shard,'
  },
  {
    id:    '13',
    label: 'claim: held rows not filtered out',
    # The claim must skip held rows, or the poison row heads every batch forever.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '            AND failed_at IS NULL' + "\n",
    replace: ''
  },
  {
    id:    '14',
    label: 'forwarder: deserialize! rescue bypassed',
    # A row this process cannot deserialize is HELD, not left to wedge the partition.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  '      immediate_jobs = immediate.map { |row| deserialize!(row) }',
    replace: '      immediate_jobs = immediate.map { |row| Serializer.deserialize(row["job_data"]) }'
  },
  {
    id:    '15',
    label: 'backoff: ceiling removed',
    # A backoff has to be clamped: Postgres intervals overflow at INT_MAX seconds.
    caught_by: 'interval_overflow_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '[seconds.to_f, MAX_BACKOFF_SECONDS].min.round(3)',
    replace: 'seconds.to_f.round(3)'
  },
  {
    id:    '16',
    label: 'backoff: interval built by text parsing',
    # Build the interval by multiplication, not by text parsing.
    caught_by: 'interval_overflow_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  'now() + ($5 * interval \'1 second\')',
    replace: 'now() + ($5 || \' seconds\')::interval'
  },
  {
    id:    '17',
    label: 'quarantine release: COLLATE dropped',
    # The quarantine release writes many partition rows too, so it needs the same
    # byte order or it crosses `stage_many!`.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"',
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_quarantine_release",'
    ].join("\n"),
    replace: [
      '              ORDER BY policy_name, partition_key',
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_quarantine_release",'
    ].join("\n")
  },
  {
    id:    '18',
    label: 'hint: pushed as a bare Hash',
    # Every hint is a `Hint` struct. This shipped as a bare Hash with a `:text` key
    # and 500'd the dashboard whenever anything was held — the exact state the hint
    # exists to surface. Rebuilt twice before it was valid Ruby; see the runner's
    # INVALID outcome, which exists because of this mutation.
    caught_by: 'operator_hints_test',
    file:  'lib/dispatch_policy/operator_hints.rb',
    find:  [
      '        hints << Hint.new(',
      '          level:   :warn,',
      '          message: "#{m[:quarantined]} staged job(s) are held back: this process could not " \\',
      '                   "deserialize them. #{tail}"',
      '        )'
    ].join("\n") + "\n",
    replace: '        hints << { level: :warn, text: "#{m[:quarantined]} staged job(s) are held back. #{tail}" }' + "\n"
  },
  {
    id:    '19',
    label: 'forwarder: rescue narrowed again',
    # The deserialize rescue must not be narrowed. It has been, and reverted, twice.
    caught_by: 'undeliverable_job_test, forwarder_deserialize_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  'rescue UnresolvableJobClass, StandardError => e',
    replace: 'rescue UnresolvableJobClass, InvalidPolicy => e'
  },
  {
    id:    '20',
    label: 'quarantine release: not sliced',
    # The release is sliced: one bind per key hits the 65,535-parameter ceiling.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  'keys.each_slice(QUARANTINE_RELEASE_BATCH) do |slice|',
    replace: '[keys].each do |slice|'
  },
  {
    id:    '21',
    label: 'sweep: aged holds never released',
    # The sweep must actually release aged holds, or a rolling deploy's backlog is
    # dropped silently and permanently.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/tick_loop.rb',
    find:  '            Repository.release_aged_quarantines!(',
    replace: '            next; Repository.release_aged_quarantines!('
  },
  {
    id:    '22',
    label: 'railtie: to_prepare reverted to after_initialize',
    # Model routing belongs in `to_prepare`, not `after_initialize`: the autoloader
    # discards the classes it configured.
    caught_by: 'connection_identity_test',
    file:  'lib/dispatch_policy/railtie.rb',
    find:  [
      '    config.to_prepare do',
      '      DispatchPolicy.route_models_to_configured_connection!',
      '    end'
    ].join("\n"),
    replace: '    # moved'
  },
  {
    id:    '23',
    label: 'dashboard tile: counts held rows as backlog',
    # Held rows are not backlog. Counting them as staged feeds a drain-time estimate
    # that can never come true. Executed by the test, not read from the source:
    # this entry used to point at the controller, where a heredoc holding the old
    # text kept the suite green while the battery printed CAUGHT.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/overview.rb',
    find:  'staged:        StagedJob.deliverable.count,',
    replace: 'staged:        StagedJob.count,'
  },
  {
    id:    '24',
    label: 'forwarder: rescue stops at NameError',
    # Stopping at NameError leaves every other deserialize failure wedging the
    # partition permanently, with every operator exit closed.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  'rescue UnresolvableJobClass, StandardError => e',
    replace: 'rescue UnresolvableJobClass, NameError, InvalidPolicy => e'
  },
  {
    id:    '25',
    label: 'quarantine release: transaction hoisted out of the slice loop',
    # One transaction per slice. Hoisting it puts the whole sweep back under one
    # FOR UPDATE hold while still emitting the same number of lock statements.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '      keys.each_slice(QUARANTINE_RELEASE_BATCH) do |slice|',
      '        connection.transaction(requires_new: true) do'
    ].join("\n"),
    replace: [
      '      connection.transaction(requires_new: true) do',
      '      keys.each_slice(QUARANTINE_RELEASE_BATCH) do |slice|'
    ].join("\n")
  },
  {
    id:    '26',
    label: 'sweep: quarantine-release rescue narrowed',
    # The quarantine release needs its own rescue, or anything it raises also skips
    # partition GC and sample retention on every sweep for the life of the process.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/tick_loop.rb',
    find:  [
      '          rescue StandardError => e',
      '            # Its own rescue:'
    ].join("\n"),
    replace: [
      '          rescue ArgumentError => e',
      '            # Its own rescue:'
    ].join("\n")
  },
  {
    id:    '27',
    label: 'hint: always promises the automatic retry',
    # The held-back hint must not promise an automatic retry when the configuration
    # disables it — a hint that lies about held rows fails exactly when it matters.
    caught_by: 'operator_hints_test',
    file:  'lib/dispatch_policy/operator_hints.rb',
    find:  '          if m.fetch(:quarantine_auto_release, true)' + "\n",
    replace: '          if true' + "\n"
  },
  {
    id:    '28',
    label: 'hint wiring: && weakened to ||',
    # Either knob at 0 means the hold never expires, so the predicate is an AND.
    caught_by: 'undeliverable_job_test',
    file:  'lib/dispatch_policy/overview.rb',
    find:  'config.quarantine_retry_after.to_i.positive? &&',
    replace: 'config.quarantine_retry_after.to_i.positive? ||'
  },
  {
    id:    '29',
    label: 'hint wiring: key deleted',
    # Deleting the key is invisible: OperatorHints defaults it to true. A SOURCE
    # pin, deliberately and unlike 23 and 28: Rails does not boot in the test
    # environment, so the controller CALL cannot be executed. The predicate it
    # calls is executable and is pinned that way — see 28.
    caught_by: 'undeliverable_job_test',
    file:  'app/controllers/dispatch_policy/dashboard_controller.rb',
    find:  '        quarantine_auto_release: Overview.quarantine_auto_release?(cfg),' + "\n",
    replace: ''
  },
  {
    id:    '30',
    label: 'forwarder: rescue enumerates NameError + TypeError',
    # Enumerating error classes is what got this rescue narrowed twice; listing the
    # ones the docs name still walks past every integration test.
    caught_by: 'forwarder_deserialize_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  'rescue UnresolvableJobClass, StandardError => e',
    replace: 'rescue UnresolvableJobClass, NameError, TypeError, InvalidPolicy => e'
  },
  {
    id:    '31',
    label: 'claim_partitions: fairness folded into the ORDER BY',
    # In-tick fairness is ordering PLUS a cap, kept apart on purpose. The claim
    # orders by last_checked_at so every pending partition is claimed within
    # ceil(N/batch) ticks; `decayed_admits` only grows when a partition ADMITS, so
    # ordering the claim by it sorts a partition that has done work behind every
    # partition that has not — permanently, above partition_batch_size candidates.
    # CLAUDE.md forbids this edit by name, and until now nothing enforced it.
    caught_by: 'claim_rotation_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  'ORDER BY last_checked_at NULLS FIRST, id',
    replace: 'ORDER BY decayed_admits ASC, last_checked_at NULLS FIRST, id'
  },
  {
    id:    '32',
    label: 'claim_partitions: last_checked_at not bumped on claim',
    # Without the bump the claim returns the same head every tick and everything
    # behind it starves — the same outcome as 31, reached from the other side.
    caught_by: 'claim_rotation_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  'SET last_checked_at = now()' + "\n",
    replace: 'SET last_checked_at = p.last_checked_at' + "\n"
  },
  {
    id:    '34',
    label: 'forwarder: rescue enumerates every documented class',
    # Same, with every documented class listed. The rule is the invariant, not the list.
    caught_by: 'forwarder_deserialize_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  'rescue UnresolvableJobClass, StandardError => e',
    replace: 'rescue UnresolvableJobClass, NameError, TypeError, KeyError, InvalidPolicy => e'
  },
  {
    id:    '35',
    label: 'pause flip: COLLATE dropped',
    # Same rule as 01, on the button an operator presses during a load spike. A
    # bare ORDER BY inherits the database collation, which is not the byte order
    # `stage_many!` sorts by — so the deadlock the sort exists to prevent comes
    # back on every install whose database is not C-collated.
    caught_by: 'pause_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '              ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"',
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_status",'
    ].join("\n"),
    replace: [
      '              ORDER BY policy_name, partition_key',
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_status",'
    ].join("\n")
  },
  {
    id:    '36',
    label: 'pause flip: ORDER BY deleted',
    # Without an order the planner locks in heap order, which is what
    # `update_all` did and what deadlocked 5 times in 12 clicks.
    caught_by: 'pause_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '              ORDER BY policy_name COLLATE "C", partition_key COLLATE "C"',
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_status",'
    ].join("\n"),
    replace: [
      '              FOR UPDATE',
      '            SQL',
      '            "lock_partitions_for_status",'
    ].join("\n")
  },
  {
    id:    '37',
    label: 'pause action: back to the unordered update_all',
    # The fix has to be reachable from the button. Reverting the controller
    # alone puts the deadlock back with the Repository method left intact and
    # unused, which is exactly how a fix ships broken.
    caught_by: 'pause_lock_order_test',
    file:  'app/controllers/dispatch_policy/policies_controller.rb',
    find:  [
      '      ran = Repository.with_policy_pause_lock(policy_name: @policy_name) do',
      '        Repository.set_policy_paused!(policy_name: @policy_name, paused: true)',
      '        Repository.set_partitions_status!(policy_name: @policy_name, status: "paused")',
      '      end',
      '      return redirect_to policy_path(@policy_name), alert: BUSY_NOTICE unless ran'
    ].join("\n"),
    replace: [
      '      Partition.transaction do',
      '        Repository.set_policy_paused!(policy_name: @policy_name, paused: true)',
      '        Partition.for_policy(@policy_name).update_all(status: "paused", updated_at: Time.current)',
      '      end'
    ].join("\n")
  },
  {
    id:    '38',
    label: 'claim_partitions: scheduled horizon back on the session clock',
    # The horizon is an application-written timestamp in a `timestamp WITHOUT time
    # zone` column. Reading it against the database session's clock is off by the
    # session TimeZone offset — scheduled work then runs early or never, silently.
    caught_by: 'scheduled_clock_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '      params      = [policy_name, DispatchPolicy.config.now]',
    replace: '      params      = [policy_name, connection.select_value("SELECT now()::timestamp")]'
  },
  {
    id:    '39',
    label: 'claim_staged_jobs!: due-time check back on the session clock',
    # Same rule one level down: this is the comparison that decides whether a
    # `set(wait:)` job may leave the staging table at all.
    caught_by: 'scheduled_clock_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '        [policy_name, partition_key, limit, DispatchPolicy.config.now]',
    replace: '        [policy_name, partition_key, limit, connection.select_value("SELECT now()::timestamp")]'
  },
  {
    id:    '40',
    label: 'defer_partition_to_next_scheduled!: park computed on the session clock',
    # Both ends of the park read `scheduled_at`. On a skewed session they move
    # together: the future row reads as due, the guard suppresses the park, and the
    # partition busy-loops every tick — M10, back again.
    caught_by: 'scheduled_clock_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '        [policy_name, partition_key, DispatchPolicy.config.now]',
    replace: '        [policy_name, partition_key, connection.select_value("SELECT now()::timestamp")]'
  },
  {
    id:    '41',
    label: 'round-trip stats: schedule-parked partitions counted as never checked',
    # A partition waiting on its own horizon is not one the tick failed to reach.
    # Counting it turns the "increase partition_batch_size or shard" hint on
    # permanently for any ordinary set(wait:) workload.
    caught_by: 'round_trip_stats_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  'COUNT(*) FILTER (WHERE p.last_checked_at IS NULL AND NOT #{PARKED_SQL})::int AS never_checked',
    replace: 'COUNT(*) FILTER (WHERE p.last_checked_at IS NULL)::int AS never_checked'
  },
  {
    id:    '42',
    label: 'adaptive: queue lag measured on the worker clock again',
    # The AIMD controller's only input. Subtracted across the worker's clock and
    # the database's, a host running fast reads every job as late, shrinks
    # current_max on every observation and never grows it back.
    caught_by: 'adaptive_clock_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  [
      '          "SELECT partition_key, GREATEST(EXTRACT(EPOCH FROM " \\',
      '          "(clock_timestamp()::timestamp - admitted_at)) * 1000, 0)::bigint AS queue_lag_ms " \\',
      '          "FROM dispatch_policy_inflight_jobs WHERE active_job_id = $1 LIMIT 1",',
      '          "lookup_admission",',
      '          [active_job_id]'
    ].join("\n"),
    replace: [
      '          "SELECT partition_key, GREATEST(EXTRACT(EPOCH FROM " \\',
      '          "($2::timestamp - admitted_at)) * 1000, 0)::bigint AS queue_lag_ms " \\',
      '          "FROM dispatch_policy_inflight_jobs WHERE active_job_id = $1 LIMIT 1",',
      '          "lookup_admission",',
      '          [active_job_id, Time.current]'
    ].join("\n")
  },
  {
    id:    '43',
    label: 'heartbeat: one thread per running job again',
    # Per job, each thread checks out its own connection against a pool the Rails
    # default sizes to the worker's thread count — every beat then queues behind
    # checkout_timeout and a still-running job gets swept as stale.
    caught_by: 'inflight_tracker_heartbeat_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '      return if @heartbeat_thread && @heartbeat_pid == Process.pid && @heartbeat_thread.alive?' + "\n\n",
    replace: ''
  },
  {
    id:    '44',
    label: 'heartbeat: one statement per running job again',
    # One thread is not enough on its own: N statements per interval is N
    # round-trips on the connection the whole fix exists to stop competing for.
    caught_by: 'inflight_tracker_heartbeat_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '        alive = beat!(ids)',
    replace: '        alive = ids.flat_map { |id| beat!([id]) || [] }'
  },
  {
    id:    '45',
    label: 'pause action: the advisory lock dropped',
    # Two overlapping clicks then interleave: a resume clears the flag while a
    # pause is still walking its slices, and the policy ends up with
    # paused=false and every partition status='paused'. Nothing admits, the
    # dashboard says the policy is running, and nothing heals it.
    caught_by: 'pause_lock_order_test',
    file:  'app/controllers/dispatch_policy/policies_controller.rb',
    find:  [
      '      ran = Repository.with_policy_pause_lock(policy_name: @policy_name) do',
      '        Repository.set_partitions_status!(policy_name: @policy_name, status: "active")',
      '        Repository.set_policy_paused!(policy_name: @policy_name, paused: false)',
      '      end',
      '      return redirect_to policy_path(@policy_name), alert: BUSY_NOTICE unless ran',
      ''
    ].join("\n"),
    replace: [
      '      Repository.set_partitions_status!(policy_name: @policy_name, status: "active")',
      '      Repository.set_policy_paused!(policy_name: @policy_name, paused: false)'
    ].join("\n")
  },
  {
    id:    '46',
    label: 'pause lock: never released',
    # A session advisory lock outlives the request on that connection. A click
    # that forgets to release it refuses every later click handled by the same
    # connection, for the life of the process — the button silently stops
    # working and says "try again in a moment" forever.
    caught_by: 'pause_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '      begin',
      '        yield',
      '      ensure',
      '        connection.select_value(',
      '          "SELECT pg_advisory_unlock(#{Integer(PAUSE_LOCK_CLASS)}, #{Integer(objid)})"',
      '        )',
      '      end',
      '      true'
    ].join("\n"),
    replace: [
      '      yield',
      '      true'
    ].join("\n")
  },
  {
    id:    '47',
    label: 'partition sweep: back to an unordered DELETE',
    # The last multi-row writer of `partitions` without a lock order. Postgres
    # usually kills the sweep, whose blanket rescue then silently skips the rest
    # of that pass, but it can pick the operator's pause click instead.
    caught_by: 'deny_lock_order_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  [
      '            ORDER BY p.policy_name COLLATE "C", p.partition_key COLLATE "C"',
      '            FOR UPDATE OF p SKIP LOCKED'
    ].join("\n"),
    replace: '            ORDER BY p.id'
  },
  {
    id:    '48',
    label: 'heartbeat: a forked child keeps the parent registry',
    # The child then beats the inflight rows of jobs running in the PARENT,
    # keeping them fresh so the stale sweeper never reclaims them — the
    # concurrency slot is lost for as long as the child lives.
    caught_by: 'inflight_tracker_heartbeat_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '        forget_inherited_registrations!' + "\n",
    replace: ''
  },
  {
    id:    '49',
    label: 'StagedJob.due back on the session clock',
    # The half of A11 that lives outside Repository. The drain button counts what
    # is left with this scope, so on a skewed session it flashes "N still pending
    # — click drain again" about rows the claim will not take, on every click.
    caught_by: 'scheduled_clock_test',
    file:  'app/models/dispatch_policy/staged_job.rb',
    find:  'where("scheduled_at IS NULL OR scheduled_at <= ?", DispatchPolicy::Repository.app_clock)',
    replace: 'where("scheduled_at IS NULL OR scheduled_at <= now()")'
  },
  {
    id:    '50',
    label: 'adaptive lag: clock_timestamp() back to now()',
    # now() is the TRANSACTION timestamp. Inside a host that wraps the perform in
    # one it stops advancing, and the queue wait becomes "time since that
    # transaction opened" — the controller never sees a job as late.
    caught_by: 'adaptive_clock_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '"(clock_timestamp()::timestamp - admitted_at)) * 1000, 0)::bigint AS queue_lag_ms " \\',
    replace: '"(now()::timestamp - admitted_at)) * 1000, 0)::bigint AS queue_lag_ms " \\'
  },
  {
    id:    '51',
    label: 'clock binding narrows config.clock to a Time',
    # config.clock is public API and every other reader calls .to_f on it, so a
    # lambda returning an epoch Float has always worked. Bound raw into SQL it
    # raises inside the admission path.
    caught_by: 'scheduled_clock_test',
    file:  'lib/dispatch_policy/repository.rb',
    find:  '      value.is_a?(Numeric) ? Time.at(value).utc : value',
    replace: '      value'
  },
  {
    id:    '52',
    label: 'heartbeat: a failed beat read as "no rows survived"',
    # nil from beat! means the database was unreachable, not that every job
    # finished. Read as an empty survivor list, ONE transient failure
    # unregisters every running job in the process permanently, and the stale
    # sweeper then deletes their rows while they run on.
    caught_by: 'inflight_tracker_heartbeat_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  '          next if alive.nil?',
    replace: '          alive ||= []'
  },
  {
    id:    '53',
    label: 'heartbeat: the loop exits on a failing cycle',
    # With a thread per job an error cost one job's heartbeat; with one thread it
    # costs every running job in the process. Only a NEW registration would
    # restart it, and a worker saturated with long jobs does not produce one.
    caught_by: 'inflight_tracker_heartbeat_test',
    file:  'lib/dispatch_policy/inflight_tracker.rb',
    find:  [
      '        rescue StandardError => e',
      '          DispatchPolicy.config.logger&.warn(',
      '            "[dispatch_policy] heartbeat cycle failed, retrying in " \\',
      '            "#{HEARTBEAT_ERROR_BACKOFF}s: #{e.class}: #{e.message}"',
      '          )',
      '          sleep HEARTBEAT_ERROR_BACKOFF'
    ].join("\n"),
    replace: [
      '        rescue StandardError => e',
      '          DispatchPolicy.config.logger&.warn(',
      '            "[dispatch_policy] heartbeat loop stopped: #{e.class}: #{e.message}"',
      '          )',
      '          retire? { true }',
      '          break'
    ].join("\n")
  },
  {
    id:    '54',
    label: 'pause action: the advisory lock dropped from #pause',
    # 45 covers the resume side. Both write the same two rows, and either one
    # racing the other produces the wedge, so a fix applied to one of them is no
    # fix at all.
    caught_by: 'pause_lock_order_test',
    file:  'app/controllers/dispatch_policy/policies_controller.rb',
    find:  [
      '      ran = Repository.with_policy_pause_lock(policy_name: @policy_name) do',
      '        Repository.set_policy_paused!(policy_name: @policy_name, paused: true)',
      '        Repository.set_partitions_status!(policy_name: @policy_name, status: "paused")',
      '      end',
      '      return redirect_to policy_path(@policy_name), alert: BUSY_NOTICE unless ran',
      ''
    ].join("\n"),
    replace: [
      '      Repository.set_policy_paused!(policy_name: @policy_name, paused: true)',
      '      Repository.set_partitions_status!(policy_name: @policy_name, status: "paused")'
    ].join("\n")
  },
    ].freeze
  end
end
