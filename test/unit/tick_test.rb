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
    assert_includes result.gate_state_patch, "throttle"
  end
end
