# frozen_string_literal: true

require_relative "../test_helper"
require "action_controller"
require "action_view"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/controllers/dispatch_policy/application_controller"

# The partition page's own logic, rendered.
#
# "A Rails view is unreachable from this suite" was written into CLAUDE.md
# as the reason a clock crossing survived there — and it is false. Rails
# does not boot here, but ERB is just a template: bind the same locals the
# controller sets and render it. A reviewer proved the point by driving the
# real page in twenty lines, and every defect this file pins was found by
# somebody else because nothing here looked.
#
# What it guards is the half of the fix that lives in the view: which
# clock each comparison uses. The numbers themselves are Repository's job
# and are tested in ScheduledClockTest.
class PartitionViewTest < DispatchPolicy::IntegrationTest
  POLICY = "view_policy"
  KEY    = "acct:1"

  VIEW = File.expand_path("../../app/views/dispatch_policy/partitions/show.html.erb", __dir__)

  def setup
    super
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build(POLICY) do
        context ->(_args) { {} }
        partition_by ->(_c) { KEY }
      end
    )
    DispatchPolicy::Repository.stage!(
      policy_name: POLICY, partition_key: KEY, queue_name: nil,
      job_class: "X", job_data: {}, context: {}
    )
  end

  # Renders the part of the template that computes for itself — everything
  # above the first `<section>` — with the locals the controller provides,
  # then appends a marker so the value of `parked` comes back in the
  # output. Kept to that prefix on purpose: the rest calls Rails helpers
  # (link_to, engine paths) that need a booted app, and every clock
  # decision is in the prefix.
  def parked_for(partition)
    source = File.read(VIEW)
    # Between the heading and the first section: the heading calls engine
    # path helpers that need a booted app, and every clock decision the
    # template makes for itself is in that gap.
    from   = source.index("</h1>") + "</h1>".length
    prefix = source[from...source.index("<section")] + "PARKED=<%= parked.inspect %>"
    facts  = DispatchPolicy::Repository.partition_clock_facts(
      policy_name: partition.policy_name, partition_key: partition.partition_key
    )

    ctx = Object.new
    ctx.instance_variable_set(:@partition, partition)
    ctx.instance_variable_set(:@clock_facts, facts)
    out = ERB.new(prefix, trim_mode: "<>").result(ctx.instance_eval { binding })

    case out[/PARKED=(\w+)/, 1]
    when "true"  then true
    when "false", "nil" then false
    else raise "the view did not render a parked value: #{out[-120..].inspect}"
    end
  end

  # `config.clock` may return an epoch Float — public API, pinned by
  # ScheduledClockTest and mutation 51. Comparing a TimeWithZone against a
  # Float does NOT raise: it compares an astronomical Julian day (~2.46e6)
  # against an epoch (~1.79e9), so it answers false for every real
  # timestamp. The page then renders "Scheduled —" beside a non-zero
  # pending count, which is the stall-looking reading that stat exists to
  # prevent, with no exception and nothing in the logs.
  def test_a_parked_partition_reads_as_parked_under_an_epoch_float_clock
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET scheduled_eligible_at = now() + interval '2 hours' " \
      "WHERE policy_name = $1 AND partition_key = $2", "park", [POLICY, KEY]
    )
    partition = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)

    [-> { Time.now.utc }, -> { Time.now.utc.to_f }].each do |clock|
      DispatchPolicy.config.clock = clock
      assert parked_for(partition),
             "a partition parked two hours out must read as parked whatever shape " \
             "config.clock returns — a Float silently answers false"
    end
  end

  def test_a_due_partition_does_not_read_as_parked
    partition = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
    refute parked_for(partition), "nothing is holding this partition back"
  end
end
