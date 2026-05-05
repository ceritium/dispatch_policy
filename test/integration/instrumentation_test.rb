# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# Top-level job class so Forwarder can `constantize` it from job_data.
class InstrumentationDemoJob < ActiveJob::Base
  queue_as :default
  def perform(*); end
end

class InstrumentationTest < Minitest::Test
  POLICY = "as_instrument_demo"

  def self.connect!
    return @connected unless @connected.nil?
    with_test_db { ActiveRecord::Base.connection.execute("SELECT 1") }
    @connected = true
  rescue StandardError
    @connected = false
  end

  def setup
    super
    skip "no Postgres available" unless self.class.connect!

    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )

    DispatchPolicy.reset_registry!
    policy = DispatchPolicy::PolicyDSL.build(POLICY) do
      context ->(args) { { tenant: args.first || "default" } }
      partition_by ->(c) { c[:tenant] }
    end
    DispatchPolicy.registry.register(policy)
  end

  def stage!(tenant)
    DispatchPolicy::Repository.stage!(
      policy_name:   POLICY,
      partition_key: tenant,
      queue_name:    nil,
      job_class:     "InstrumentationDemoJob",
      job_data:      { "job_id" => "as-#{tenant}-#{rand(10_000)}",
                       "job_class" => "InstrumentationDemoJob",
                       "arguments" => [tenant] },
      context:       { tenant: tenant },
      priority:      0
    )
  end

  def test_tick_event_fires_with_expected_payload
    captured = []
    sub = ActiveSupport::Notifications.subscribe("tick.dispatch_policy") do |_n, _s, _f, _id, payload|
      captured << payload.dup
    end

    stage!("acme")
    DispatchPolicy::Tick.run(policy_name: POLICY)

    refute_empty captured, "tick.dispatch_policy never fired"
    payload = captured.last
    assert_equal POLICY, payload[:policy_name].to_s
    %i[duration_ms partitions_seen jobs_admitted forward_failures].each do |k|
      assert payload.key?(k), "tick payload missing #{k}: #{payload.inspect}"
    end
  ensure
    ActiveSupport::Notifications.unsubscribe(sub) if sub
  end

  def test_pipeline_admit_fires_when_a_partition_admits
    admits = []
    sub = ActiveSupport::Notifications.subscribe("pipeline_admit.dispatch_policy") do |_n, _s, _f, _id, payload|
      admits << payload.dup
    end

    stage!("acme")
    DispatchPolicy::Tick.run(policy_name: POLICY)

    refute_empty admits, "pipeline_admit.dispatch_policy never fired"
    assert_equal "acme", admits.first[:partition_key]
    assert_operator admits.first[:admitted], :>=, 1
  ensure
    ActiveSupport::Notifications.unsubscribe(sub) if sub
  end
end
