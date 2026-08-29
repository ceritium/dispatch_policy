# frozen_string_literal: true

require_relative "../test_helper"

# Smoke test: builds a Pipeline and exercises decisions. The end-to-end
# Tick path is verified in integration tests against Postgres.
class TickPipelineSmokeTest < Minitest::Test
  def test_pipeline_combines_gates
    policy = DispatchPolicy::PolicyDSL.build("p") do
      context ->(args) { { rate: args.first || 5 } }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: ->(c) { c[:rate] }, per: 60
    end

    pipeline = DispatchPolicy::Pipeline.new(policy)
    partition = { "policy_name" => "p", "partition_key" => "k", "gate_state" => {} }
    result = pipeline.call(DispatchPolicy::Context.wrap({ rate: 4 }), partition, 100)

    assert_equal 4, result.admit_count

    # The throttle settles its bucket in the admission UPDATE rather than
    # handing back a literal patch computed from an earlier read, so it
    # contributes a charge and nothing to gate_state_patch.
    refute_includes result.gate_state_patch, "throttle"
    charge = DispatchPolicy::Pipeline.charge_for(result.decisions)
    refute_nil charge, "the throttle must carry the numbers its charge needs"
    assert_in_delta 4.0,      charge[:capacity],    0.001
    assert_in_delta 4 / 60.0, charge[:refill_rate], 0.001
  end
end
