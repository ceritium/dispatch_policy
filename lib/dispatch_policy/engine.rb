# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
  class UnsupportedAdapterError < StandardError; end

  class Engine < ::Rails::Engine
    isolate_namespace DispatchPolicy

    initializer "dispatch_policy.reference_patch" do
      config.to_prepare do
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
      MSG
    end

    initializer "dispatch_policy.boot_prune", after: :load_config_initializers do
      config.to_prepare do
        begin
          DispatchPolicy::Dispatch.purge_drained_partitions
        rescue ActiveRecord::NoDatabaseError,
               ActiveRecord::StatementInvalid,
               ActiveRecord::ConnectionNotEstablished
          # DB not ready — skip silently.
        end
      end
    end
  end
end
