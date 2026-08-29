# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/inflight_job"

# The Tick pre-inserts an inflight row for every admitted job, and
# InflightTracker.track's `ensure` is what removes it. A job that dies
# BEFORE around_perform never reaches that ensure — argument
# deserialization is the routine case, when a GlobalID's record was
# deleted between enqueue and perform.
#
# `discard.active_job` does not cover it. That notification is emitted by
# exactly one thing in ActiveJob: the rescue_from handler `discard_on`
# installs. A job class with no handler dies in perform_now's bare
# `rescue Exception` and emits no discard at all, so the row orphaned
# until the queued sweeper an hour later — with `gate :concurrency,
# max: 1` that is an hour of a frozen tenant, per such job.
class FailedPerformCleanupTest < DispatchPolicy::IntegrationTest
  class ExplodingJob < ActiveJob::Base
    include DispatchPolicy::JobExtension

    POLICY = dispatch_policy("failed_perform") do
      context ->(_args) { {} }
      partition_by ->(_c) { "k" }
      gate :concurrency, max: 1
    end

    # Stands in for a failure during argument deserialization: it happens
    # inside perform_now but before the perform callbacks, so `track`'s
    # ensure never runs, and there is no discard_on to report it.
    def perform(*)
      raise StandardError, "record gone"
    rescue StandardError
      raise ActiveJob::DeserializationError
    end
  end

  def setup
    super
    DispatchPolicy.registry.register(ExplodingJob::POLICY, owner: ExplodingJob.name)
    # What the railtie installs; the test environment does not boot Rails.
    @subscriber = ActiveSupport::Notifications.subscribe("perform.active_job") do |event|
      next unless event.payload[:exception]

      DispatchPolicy::InflightTracker.handle_discard(event.payload[:job])
    end
  end

  def teardown
    ActiveSupport::Notifications.unsubscribe(@subscriber)
    super
  end

  def test_a_job_that_dies_without_discard_on_still_releases_its_slot
    job = ExplodingJob.new
    DispatchPolicy::Repository.insert_inflight!([{
      policy_name: "failed_perform", partition_key: "k", active_job_id: job.job_id
    }])
    assert_equal 1, DispatchPolicy::InflightJob.count

    assert_raises(ActiveJob::DeserializationError) { job.perform_now }

    assert_equal 0, DispatchPolicy::InflightJob.count,
                 "nothing else reaps this row for an hour, and the partition is at max"
  end
  # The behavioural test above installs the subscription itself (the test
  # environment does not boot Rails), so pin the wiring separately: the
  # railtie has to subscribe to perform.active_job, not only to discard.
  def test_the_railtie_subscribes_to_failed_performs
    source = File.read(File.expand_path("../../lib/dispatch_policy/railtie.rb", __dir__))
    block  = source[/discard_cleanup.*?\n    end/m]

    refute_nil block
    assert_includes block, "perform.active_job"
    assert_includes block, "discard.active_job"
  end

  # A job whose arguments cannot be rebuilt must not raise out of the
  # ENQUEUE callback. ActiveJob's own enqueue copes — serialize reuses
  # @serialized_arguments — so raising here destroys the retry that
  # `retry_on ActiveJob::DeserializationError` had just scheduled, turning
  # a recoverable job into a hard failure the gem itself caused.
  def test_an_unrebuildable_argument_is_handed_to_the_adapter_not_raised
    raiser = lambda do |_job|
      raise StandardError, "record gone"
    rescue StandardError
      raise ActiveJob::DeserializationError
    end

    before = ActiveJob::Base.queue_adapter.enqueued_jobs.size
    DispatchPolicy::JobExtension.stub(:ensure_arguments_materialized!, raiser) do
      ExplodingJob.perform_later
    end

    assert_equal 0, DispatchPolicy::StagedJob.count,
                 "we could not compute a partition, so the gem steps aside"
    assert_equal before + 1, ActiveJob::Base.queue_adapter.enqueued_jobs.size,
                 "the adapter gets it and it fails at perform — the un-gemmed behaviour"
  end
end
