# frozen_string_literal: true

require_relative "../test_helper"
require "action_controller"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/controllers/dispatch_policy/application_controller"
require_relative "../../app/controllers/dispatch_policy/partitions_controller"

# The drain button is what an operator reaches for when something is
# already wrong, so it is exactly the wrong place to have no error
# isolation. One staged row the Forwarder cannot deserialize — a job
# class renamed or deleted in a deploy while its rows are still staged —
# raised NameError out of the controller as a bare 500: no flash, no
# partition name, no count, nothing drained. And because the poison
# partition sorts first, every healthy partition behind it in the
# policy-wide drain was never reached, identically on every click.
class DrainIsolationTest < DispatchPolicy::IntegrationTest
  class DrainJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("drain_isolation") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(DrainJob::POLICY, owner: DrainJob.name)
    DrainJob.perform_later
    @partition = DispatchPolicy::Partition.first
  end

  def test_a_partition_whose_forward_raises_is_reported_not_raised
    poison = ->(**) { raise NameError, "uninitialized constant OldJob" }

    drained, _due, _scheduled, failed =
      DispatchPolicy::ManualAdmission.stub(:force!, poison) do
        DispatchPolicy::PartitionsController.drain_partition!(@partition)
      end

    assert_equal 0, drained
    assert failed, "the caller has to learn this partition was abandoned, not get a 500"
    assert_equal 1, DispatchPolicy::StagedJob.count,
                 "the claim TX rolled back, so the staged row is still there"
  end

  def test_a_healthy_partition_reports_no_failure
    drained, _due, _scheduled, failed =
      DispatchPolicy::PartitionsController.drain_partition!(@partition)

    assert_equal 1, drained
    refute failed
  end
end
