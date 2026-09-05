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

  # Seeded with the same expression the gem writes with, so the only
  # thing a skewed session can change is whether the READ is right.
  UTC = DispatchPolicy::Repository::UTC_NOW

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
  # Every value the template computes for itself, as the template computes
  # it. Markers are appended rather than parsed out of the rendered markup
  # so that a template edit changes the ANSWER rather than silently
  # changing what is being measured.
  MARKERS = "|PARKED=<%= parked.inspect %>" \
            "|BACKOFF=<%= in_backoff.inspect %>" \
            "|AGE=<%= age_seconds.inspect %>" \
            "|EWMA=<%= decayed_now.round(6) %>|"

  def render_header(partition)
    source = File.read(VIEW)
    # Between the heading and the first section: the heading calls engine
    # path helpers that need a booted app, and every clock decision the
    # template makes for itself is in that gap.
    head = source.index("</h1>")
    sect = source.index("<section")
    raise "the template no longer has the shape this test slices" unless head && sect

    prefix = source[(head + "</h1>".length)...sect] + MARKERS
    facts  = DispatchPolicy::Repository.partition_clock_facts(
      policy_name: partition.policy_name, partition_key: partition.partition_key
    )

    ctx = Object.new
    ctx.instance_variable_set(:@partition, partition)
    ctx.instance_variable_set(:@clock_facts, facts)
    out = ERB.new(prefix, trim_mode: "<>").result(ctx.instance_eval { binding })

    # Split on the separator rather than matching it: a regex that CONSUMES
    # the trailing `|` leaves the next marker without its leading one and
    # captures every other value.
    values = out.split("|").filter_map { |part| part.split("=", 2) if part.include?("=") }.to_h
    raise "the view rendered no markers: #{out[-160..].inspect}" if values.size < 4

    { parked:      boolish(values.fetch("PARKED")),
      in_backoff:  boolish(values.fetch("BACKOFF")),
      age_seconds: values.fetch("AGE") == "nil" ? nil : values.fetch("AGE").to_f,
      ewma:        values.fetch("EWMA").to_f,
      facts:       facts }
  end

  def boolish(text)
    case text
    when "true" then true
    when "false", "nil" then false
    else raise "not a boolean: #{text.inspect}"
    end
  end

  def parked_for(partition)
    render_header(partition)[:parked]
  end

  def partition_row
    DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
  end

  def session_timezone(zone)
    DispatchPolicy::Repository.connection.execute("SET TIME ZONE '#{zone}'")
  end

  # SET TIME ZONE is a SESSION setting and these cases share a connection,
  # so without this the skewed case contaminates whatever runs next — which
  # it did: the expired-horizon test passed alone and failed in the full
  # suite, because its `#{UTC} - interval '2 hours'` was being written and
  # read ten hours apart.
  def teardown
    session_timezone("UTC")
    super
  end

  # `config.clock` may return an epoch Float — public API, pinned by
  # ScheduledClockTest and mutation 51. Comparing a TimeWithZone against a
  # Float does NOT raise: it compares an astronomical Julian day (~2.46e6)
  # against an epoch (~1.79e9), so it answers false for every real
  # timestamp. The page then renders "Scheduled —" beside a non-zero
  # pending count, which is the stall-looking reading that stat exists to
  # prevent, with no exception and nothing in the logs.
  def test_a_parked_partition_reads_as_parked_under_an_epoch_float_clock
    set_horizon!("#{UTC} + interval '2 hours'")
    partition = partition_row

    [-> { Time.now.utc }, -> { Time.now.utc.to_f }].each do |clock|
      DispatchPolicy.config.clock = clock
      assert parked_for(partition),
             "a partition parked two hours out must read as parked whatever shape " \
             "config.clock returns — a Float silently answers false"
    end
  end

  # A partition with NO horizon at all. Kept as its own case because it is
  # what pins the nil guard: drop `@partition.scheduled_eligible_at &&` and
  # the page raises NoMethodError for every partition without scheduled
  # work, which is most of them.
  def test_a_partition_with_no_horizon_does_not_read_as_parked
    refute parked_for(partition_row), "nothing is holding this partition back"
  end

  # And one whose horizon has PASSED, which is the case that actually
  # exercises the comparison — the test above short-circuits on the nil and
  # never reaches it. Ordinary state: `defer_partition_to_next_scheduled!`
  # writes a future horizon and nothing rewrites it as it expires, so every
  # parked partition sits here between its horizon and the next tick.
  def test_a_partition_whose_horizon_has_passed_does_not_read_as_parked
    set_horizon!("#{UTC} - interval '2 hours'")
    refute parked_for(partition_row),
           "the horizon passed two hours ago; this partition is due, not parked"
  end

  # The point of the whole change: the view must READ the facts the
  # database computed, not recompute them from `Time.current`. Under a
  # skewed session those disagree — which is the A10/A11 crossing — and a
  # test that only checks `parked` cannot see it: reverting this template
  # wholesale to the pre-fix version left the entire suite green.
  #
  # East of UTC, a Postgres-written timestamp reads as being in the future
  # on the app clock: the age goes negative and the decay is skipped.
  def test_the_template_reads_the_database_facts_rather_than_recomputing_them
    session_timezone("Etc/GMT-10")
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET decayed_admits = 10.0, " \
      "decayed_admits_at = #{UTC} - interval '600 seconds', " \
      "last_checked_at   = #{UTC} - interval '30 seconds', " \
      "next_eligible_at  = #{UTC} + interval '300 seconds' " \
      "WHERE policy_name = $1 AND partition_key = $2",
      "seed", [POLICY, KEY]
    )

    rendered = render_header(partition_row)

    assert_in_delta 30, rendered[:age_seconds], 5,
                    "recomputed from Time.current this renders as minus ten hours"
    assert_operator rendered[:ewma], :<, 1.0,
                    "10 admits decayed over 600s is ~0.0098; recomputed, the decay is " \
                    "skipped and the page renders 10.00"
    assert_equal rendered[:facts][:age_seconds].round(3), rendered[:age_seconds].round(3),
                 "the page must show exactly what the database computed"
    assert_equal rendered[:facts][:in_backoff], rendered[:in_backoff],
                 "the page must show the database's backoff answer, not its own"
  end

  # `in_backoff` needs its own case, because the one above cannot
  # discriminate it: EAST of UTC a live backoff reads as live whether the
  # page recomputes it or reads it, so `assert rendered[:in_backoff]` there
  # passes against the bug — which is exactly the defect this branch fixed
  # in scheduled_clock_test and then reproduced here. A backoff that has
  # EXPIRED is the shape that separates them: recomputed east, it still
  # reads as active.
  def test_an_expired_backoff_does_not_render_as_active
    session_timezone("Etc/GMT-10")
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET next_eligible_at = #{UTC} - interval '300 seconds' " \
      "WHERE policy_name = $1 AND partition_key = $2",
      "expired", [POLICY, KEY]
    )

    refute render_header(partition_row)[:in_backoff],
           "the backoff expired five minutes ago; recomputed on the app clock the page " \
           "still shows it as active"
  end

  private

  def set_horizon!(sql_expression)
    DispatchPolicy::Repository.connection.exec_query(
      "UPDATE dispatch_policy_partitions SET scheduled_eligible_at = #{sql_expression} " \
      "WHERE policy_name = $1 AND partition_key = $2", "horizon", [POLICY, KEY]
    )
  end
end
