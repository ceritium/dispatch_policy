# frozen_string_literal: true

require "rails/railtie"

module DispatchPolicy
  class Railtie < ::Rails::Railtie
    initializer "dispatch_policy.active_job" do
      ActiveSupport.on_load(:active_job) do
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

    # Reap the inflight row when a job is discarded before its perform
    # callbacks run (e.g. discard_on ActiveJob::DeserializationError):
    # InflightTracker.track's `ensure` never fires in that path, so the
    # Tick's pre-inserted row would orphan until the stale sweeper.
    initializer "dispatch_policy.discard_cleanup" do
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
