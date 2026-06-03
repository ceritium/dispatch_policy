# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# The throttle must charge its token bucket for jobs ACTUALLY admitted,
# not for the optimistic `allowed` it returned at evaluate time. Fewer
# jobs than `allowed` get admitted when:
#   - some staged rows are scheduled in the future (counted in
#     pending_count, skipped by the `scheduled_at <= now()` filter), or
#   - a later gate (concurrency) caps admit_count below the throttle's
#     allowance, or
#   - another tick grabbed rows under SKIP LOCKED.
# Before the fix the bucket was drained by `allowed` regardless, leaking
# tokens and dragging the effective rate below the configured one.
class ThrottleChargeTest < Minitest::Test
  class ChargeJob < ActiveJob::Base
    include DispatchPolicy::JobExtension
    def perform(*); end
  end

  def self.connect!
    return @connected if defined?(@connected) && @connected

    ActiveRecord::Base.establish_connection(
      adapter:  "postgresql",
      encoding: "unicode",
      host:     ENV.fetch("DB_HOST", "localhost"),
      username: ENV.fetch("DB_USER", ENV["USER"]),
      password: ENV.fetch("DB_PASS", ""),
      database: ENV.fetch("DB_NAME", "dispatch_policy_test")
    )
    ActiveRecord::Base.connection.execute("SELECT 1")
    @connected = true
  rescue StandardError => e
    warn "[skip] Postgres not reachable: #{e.message}"
    @connected = false
  end

  def setup
    super
    skip "no Postgres available" unless self.class.connect!
    truncate_tables!
    DispatchPolicy.reset_registry!
  end

  def teardown
    DispatchPolicy.reset_registry!
  end

  def truncate_tables!
    ActiveRecord::Base.connection.execute(
      "TRUNCATE dispatch_policy_staged_jobs, dispatch_policy_partitions, " \
      "dispatch_policy_inflight_jobs, dispatch_policy_tick_samples RESTART IDENTITY"
    )
  end

  def stage!(scheduled_at: nil, id:)
    DispatchPolicy::Repository.stage!(
      policy_name:   "charge",
      partition_key: "k",
      queue_name:    nil,
      job_class:     "ThrottleChargeTest::ChargeJob",
      job_data:      { "job_id" => id, "job_class" => "ThrottleChargeTest::ChargeJob", "arguments" => [{}] },
      context:       {},
      scheduled_at:  scheduled_at,
      priority:      0
    )
  end

  def tokens_left
    DispatchPolicy::Partition.find_by(partition_key: "k").gate_state.dig("throttle", "tokens").to_f
  end

  # ----- future-scheduled rows ------------------------------------------

  def test_throttle_only_charges_for_immediately_claimable_rows
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build("charge") do
        context ->(_args) { {} }
        partition_by ->(_c) { "k" }
        gate :throttle, rate: 10, per: 60
      end
    )
    ChargeJob.dispatch_policy_name = "charge"

    3.times { |i| stage!(id: "now-#{i}") }
    4.times { |i| stage!(id: "future-#{i}", scheduled_at: 1.hour.from_now) }

    result = DispatchPolicy::Tick.run(policy_name: "charge")

    # Only the 3 immediate jobs are admitted; the throttle is charged 3,
    # leaving ~7 tokens. The bug charged `allowed` (10), draining to 0.
    assert_equal 3, result.jobs_admitted
    assert_in_delta 7.0, tokens_left, 0.1,
                    "bucket must be charged for the 3 admitted, not the 10 allowed"

    partition = DispatchPolicy::Partition.find_by(partition_key: "k")
    assert_equal 3, partition.total_admitted
    assert_equal 4, partition.pending_count, "the 4 future-scheduled jobs stay pending"
  end

  # ----- a later gate caps admit_count ----------------------------------

  def test_throttle_only_charges_for_what_a_downstream_concurrency_gate_allows
    DispatchPolicy.registry.register(
      DispatchPolicy::PolicyDSL.build("charge") do
        context ->(_args) { {} }
        partition_by ->(_c) { "k" }
        gate :throttle,    rate: 100, per: 60
        gate :concurrency, max: 2
      end
    )
    ChargeJob.dispatch_policy_name = "charge"

    10.times { |i| stage!(id: "j-#{i}") }

    result = DispatchPolicy::Tick.run(policy_name: "charge")

    # concurrency caps admit_count at 2 (0 in-flight, max 2). The throttle
    # allowed 100, but only 2 are admitted — so it must be charged 2,
    # leaving ~98. The bug charged the throttle's `allowed` (100), draining
    # the bucket to 0 even though only 2 jobs went out.
    assert_equal 2, result.jobs_admitted
    assert_in_delta 98.0, tokens_left, 0.1,
                    "bucket must be charged for the 2 admitted, not the throttle's 100 allowance"
    assert_equal 2, DispatchPolicy::InflightJob.where(policy_name: "charge").count
  end
end
