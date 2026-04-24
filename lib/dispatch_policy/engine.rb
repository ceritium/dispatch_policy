# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
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

    initializer "dispatch_policy.boot_prune", after: :load_config_initializers do
      config.to_prepare do
        begin
          DispatchPolicy::Tick.prune_orphan_gate_rows
          DispatchPolicy::Tick.prune_idle_partitions
        rescue ActiveRecord::NoDatabaseError,
               ActiveRecord::StatementInvalid,
               ActiveRecord::ConnectionNotEstablished
          # DB not ready — skip silently.
        end
      end
    end
  end
end
