# frozen_string_literal: true

require "rails/railtie"

module DispatchPolicy
  class Railtie < ::Rails::Railtie
    initializer "dispatch_policy.active_job" do
      ActiveSupport.on_load(:active_job) do
        # Brings InflightTracker with it (JobExtension declares it as a
        # Concern dependency), so every job class in the host app can both
        # be staged and have its inflight row released — including classes
        # bound to a policy through `dispatch_policy_name=` rather than the
        # `dispatch_policy` macro.
        include DispatchPolicy::JobExtension
      end

      ActiveSupport.on_load(:active_job) do
        if defined?(ActiveJob) && ActiveJob.respond_to?(:perform_all_later)
          singleton = ActiveJob.singleton_class
          unless singleton.include?(DispatchPolicy::JobExtension::BulkEnqueue)
            singleton.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
          end
        end
      end
    end

    # Reap the inflight row when a job dies before its perform callbacks
    # run: InflightTracker.track's `ensure` never fires in that path, so
    # the Tick's pre-inserted row would orphan until the stale sweeper —
    # an hour of a `:concurrency` partition wedged one slot short, per
    # such job.
    #
    # `discard.active_job` alone does NOT cover that. It is instrumented
    # by exactly one thing in ActiveJob: the rescue_from handler that
    # `discard_on` installs. A job class with no handler dies in
    # perform_now's bare `rescue Exception` and emits no discard at all —
    # so the routine case (a GlobalID argument whose record was deleted
    # between enqueue and perform, raising ActiveJob::DeserializationError
    # during argument deserialization) was uncovered unless the host
    # happened to have declared `discard_on`.
    #
    # `perform.active_job` wraps the whole of perform_now, argument
    # deserialization included, and carries an :exception payload when the
    # job dies. Deleting on it is idempotent and safe: on the normal path
    # `track`'s ensure has already removed the row, and the Tick
    # regenerates active_job_id on every admission, so a late delete can
    # never reach the row of a re-staged job's NEXT admission.
    initializer "dispatch_policy.discard_cleanup" do
      ActiveSupport::Notifications.subscribe("perform.active_job") do |event|
        next unless event.payload[:exception]

        DispatchPolicy::InflightTracker.handle_discard(event.payload[:job])
      end

      ActiveSupport::Notifications.subscribe("discard.active_job") do |event|
        DispatchPolicy::InflightTracker.handle_discard(event.payload[:job])
      end
    end

    # Hosts copy the gem's migration into their own db/migrate via
    # `rails railties:install:migrations` (or hand-write a cutover
    # migration like opstasks did). We deliberately do NOT auto-merge
    # the gem's db/migrate into the host's lookup paths — that
    # surfaces an `ActiveRecord::DuplicateMigrationNameError` for
    # any host already carrying a migration named
    # `CreateDispatchPolicyTables` (e.g. one copied from the
    # upstream tick-hardening branch during a cutover).

    config.after_initialize do
      DispatchPolicy.warn_unsupported_adapter
    end
  end
end
