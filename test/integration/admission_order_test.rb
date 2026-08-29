# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# The staged claim carries the host's ActiveJob priority verbatim, so it
# has to spend it the way ActiveJob does. Both supported adapters agree
# that a SMALLER number is more urgent — good_job's `priority_ordered` is
# "priority ASC NULLS LAST", solid_queue's `ordered` is "priority: :asc",
# and that is what `set(priority:)` means to a host. Ordering the claim
# DESC admitted the least urgent work first, and behind a steady stream
# of default-priority jobs an urgent one is starved indefinitely.
class AdmissionOrderTest < DispatchPolicy::IntegrationTest
  class OrderedJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("admission_order") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :throttle, rate: 1, per: 3600 # one admission, so order is all that matters
    end

    def perform(*); end
  end

  def setup
    super
    DispatchPolicy.registry.register(OrderedJob::POLICY, owner: OrderedJob.name)
  end

  def stage!(priority)
    DispatchPolicy::Repository.stage!(
      policy_name:   "admission_order",
      partition_key: "k",
      queue_name:    nil,
      job_class:     OrderedJob.name,
      job_data:      OrderedJob.new.serialize.merge("priority" => priority),
      context:       {},
      priority:      priority
    )
  end

  def test_the_most_urgent_job_is_admitted_first
    stage!(10)  # bulk work
    stage!(0)   # default
    stage!(-10) # urgent

    DispatchPolicy::Tick.run(policy_name: "admission_order")

    left = DispatchPolicy::StagedJob.order(:id).pluck(:priority)
    assert_equal [10, 0], left,
                 "the -10 job is the urgent one; admitting 10 first is the host's " \
                 "priority read backwards"
  end
end
