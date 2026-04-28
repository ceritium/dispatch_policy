# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
  # Raised at boot when the host app is configured with an ActiveJob
  # adapter that cannot participate in the staging transaction (Sidekiq,
  # Resque, SQS, ...). dispatch_policy's atomicity guarantees only hold
  # for adapters that share a PostgreSQL connection with our tables.
  class UnsupportedAdapterError < StandardError; end

  class Engine < ::Rails::Engine
    isolate_namespace DispatchPolicy

    initializer "dispatch_policy.reference_gates" do
      config.to_prepare do
        # Reference the built-in gates so they register in Gate.registry.
        DispatchPolicy::Gates::Concurrency
        DispatchPolicy::Gates::Throttle
        DispatchPolicy::Gates::GlobalCap
        DispatchPolicy::Gates::FairInterleave
        DispatchPolicy::Gates::AdaptiveConcurrency

        DispatchPolicy::ActiveJobPerformAllLaterPatch
      end
    end

    initializer "dispatch_policy.adapter_guard", after: :load_config_initializers do
      next unless DispatchPolicy.enabled?

      adapter = ActiveJob::Base.queue_adapter_name&.to_sym
      next if adapter.nil?

      allowed = DispatchPolicy.config.allowed_adapters ||
                (PG_BACKED_ADAPTERS + IN_PROCESS_ADAPTERS)

      next if allowed.include?(adapter)

      raise UnsupportedAdapterError, <<~MSG.squish
        DispatchPolicy requires a PostgreSQL-backed ActiveJob adapter
        (good_job, solid_queue) sharing the same connection as
        dispatch_policy_staged_jobs so admission and enqueue can run in
        a single transaction. Detected adapter: #{adapter.inspect}.
        See README → "Adapter requirements". To override (at your own
        risk), set DispatchPolicy.config.allowed_adapters.
      MSG
    end

    initializer "dispatch_policy.boot_prune", after: :load_config_initializers do
      config.to_prepare do
        begin
          DispatchPolicy::Tick.prune_orphan_gate_rows
          DispatchPolicy::Tick.prune_idle_partitions
          DispatchPolicy::PartitionObservation.prune!
        rescue ActiveRecord::NoDatabaseError,
               ActiveRecord::StatementInvalid,
               ActiveRecord::ConnectionNotEstablished
          # DB not ready — skip silently.
        end
      end
    end
  end
end
