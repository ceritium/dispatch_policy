# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# H4: `config.enabled = false` turns off STAGING, not admission.
#
# It used to break the tick loop out of its own loop as well, which left
# everything already in staged_jobs unreachable: new enqueues went
# straight to the adapter, and the only thing that hands staged rows to
# the adapter — the tick — had stopped. The backlog sat there until an
# operator found the dashboard's drain button, which is the opposite of
# what config.rb advertises ("drain the staging table without taking
# traffic offline").
class MasterSwitchTest < DispatchPolicy::IntegrationTest
  class SwitchJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("master_switch") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(SwitchJob::POLICY, owner: SwitchJob.name)
  end

  def teardown
    DispatchPolicy.config.enabled = true
    super
  end

  def test_disabling_stops_staging_but_still_drains_what_is_staged
    3.times { SwitchJob.perform_later }
    assert_equal 3, DispatchPolicy::StagedJob.count

    DispatchPolicy.config.enabled = false

    # New work bypasses admission entirely — that is the point of the flag.
    SwitchJob.perform_later
    assert_equal 3, DispatchPolicy::StagedJob.count,
                 "a disabled gem must hand new jobs straight to the adapter"

    iterations = 0
    DispatchPolicy::TickLoop.run(
      policy_name: "master_switch",
      stop_when:   -> { (iterations += 1) > 3 }
    )

    assert_equal 0, DispatchPolicy::StagedJob.count,
                 "the backlog must still drain; stranding it leaves the rows reachable " \
                 "only from the dashboard's drain button"
  end

  # The flag is read per iteration, so flipping it mid-flight must not
  # need a restart of the tick job in either direction.
  def test_enabling_again_resumes_staging
    DispatchPolicy.config.enabled = false
    SwitchJob.perform_later
    assert_equal 0, DispatchPolicy::StagedJob.count

    DispatchPolicy.config.enabled = true
    SwitchJob.perform_later
    assert_equal 1, DispatchPolicy::StagedJob.count
  end
end
