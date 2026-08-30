# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# The gates key their state on the partition ROW, not on a value
# recomputed from ctx at evaluate time. The two agree right up until
# somebody edits `partition_by` — and then every row the admission path
# already wrote under the stored key becomes invisible to a count taken
# under the recomputed one, so the cap silently stops applying to every
# partition that predates the edit. An edit like that is an ordinary
# deploy: renaming the expression, coarsening the scope, adding a prefix.
class LivePolicyEditTest < DispatchPolicy::IntegrationTest
  POLICY = "live_edit"

  def build_policy(prefix)
    DispatchPolicy::PolicyDSL.build(POLICY) do
      context ->(args) { { account: args.first } }
      partition_by ->(c) { "#{prefix}:#{c[:account]}" }
      gate :concurrency, max: 2
    end
  end

  def register!(prefix)
    DispatchPolicy.reset_registry!
    DispatchPolicy.registry.register(build_policy(prefix))
  end

  def teardown
    DispatchPolicy.reset_registry!
    super
  end

  def test_the_cap_still_applies_after_partition_by_is_edited
    register!("acct")

    # A partition that exists, holds work, and has two jobs in flight.
    5.times do
      DispatchPolicy::Repository.stage!(
        policy_name: POLICY, partition_key: "acct:1", queue_name: nil,
        job_class: "X", job_data: {}, context: { "account" => 1 }
      )
    end
    DispatchPolicy::Repository.insert_inflight!([
      { policy_name: POLICY, partition_key: "acct:1", active_job_id: "a" },
      { policy_name: POLICY, partition_key: "acct:1", active_job_id: "b" }
    ])

    # The deploy renames the expression. The partition row keeps its key.
    register!("tenant")

    gate      = DispatchPolicy.registry.fetch(POLICY).gates.first
    partition = DispatchPolicy::Repository.normalize_partition(
      ActiveRecord::Base.connection.exec_query(
        "SELECT * FROM dispatch_policy_partitions WHERE partition_key = 'acct:1'"
      ).first
    )
    ctx = DispatchPolicy.registry.fetch(POLICY).build_context([1])

    decision = gate.evaluate(ctx, partition, 100)

    assert_equal 0, decision.allowed,
                 "two of two slots are in flight; counting under the recomputed " \
                 "key finds zero and hands out the cap all over again"
    assert_equal "concurrency_full", decision.reason
  end

  # A cap that came through jsonb is not necessarily an Integer any more.
  # The README's own example backs it with a host DB column, and a numeric
  # column arrives as the String "5.0" — on which Integer() raises, inside
  # the admission TX, wedging the partition behind forward_failure_backoff.
  def test_a_cap_retyped_by_the_jsonb_round_trip_still_works
    gate = DispatchPolicy::Gates::Concurrency.new(max: ->(c) { c[:cap] })
    partition = { "policy_name" => POLICY, "partition_key" => "k", "gate_state" => {} }

    decision = gate.evaluate(DispatchPolicy::Context.wrap({ cap: "5.0" }), partition, 100)

    assert_equal 5, decision.allowed
  end
end
