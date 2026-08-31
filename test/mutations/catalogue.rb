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
      "04" => "Unreachable by construction: the horizon MIN only runs for a " \
              "partition the claim already accepted, and the claim requires " \
              "pending_count > 0, which the quarantine decrements to zero. " \
              "No caller can reach it with a held row in scope."
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
    find:  '[ts.is_a?(Time) ? ts : Time.parse(ts.to_s), row["partition_key"]]',
    replace: '[ts.is_a?(Time) ? ts : Time.parse(ts.to_s), nil]'
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
    caught_by: 'interval_overflow_test',
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
    # that can never come true.
    caught_by: 'undeliverable_job_test',
    file:  'app/controllers/dispatch_policy/dashboard_controller.rb',
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
    # Either knob at 0 means the hold never expires, so the wiring is an AND.
    caught_by: 'undeliverable_job_test',
    file:  'app/controllers/dispatch_policy/dashboard_controller.rb',
    find:  'quarantine_auto_release: cfg.quarantine_retry_after.to_i.positive? &&',
    replace: 'quarantine_auto_release: cfg.quarantine_retry_after.to_i.positive? ||'
  },
  {
    id:    '29',
    label: 'hint wiring: key deleted',
    # Deleting the key is invisible: OperatorHints defaults it to true.
    caught_by: 'undeliverable_job_test',
    file:  'app/controllers/dispatch_policy/dashboard_controller.rb',
    find:  [
      '        quarantine_auto_release: cfg.quarantine_retry_after.to_i.positive? &&',
      '                                 cfg.sweep_every_ticks.to_i.positive?,'
    ].join("\n") + "\n",
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
    id:    '34',
    label: 'forwarder: rescue enumerates every documented class',
    # Same, with every documented class listed. The rule is the invariant, not the list.
    caught_by: 'forwarder_deserialize_test',
    file:  'lib/dispatch_policy/forwarder.rb',
    find:  'rescue UnresolvableJobClass, StandardError => e',
    replace: 'rescue UnresolvableJobClass, NameError, TypeError, KeyError, InvalidPolicy => e'
  },
    ].freeze
  end
end
