# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    before_action :find_policy, only: %i[show pause resume]

    def index
      registry_names = DispatchPolicy.registry.names
      db_names       = Partition.distinct.pluck(:policy_name)
      names          = (registry_names + db_names).uniq.sort

      @rows = names.map do |name|
        partitions = Partition.for_policy(name)
        {
          name:           name,
          registered:     registry_names.include?(name),
          pending:        partitions.sum(:pending_count),
          in_flight:      partitions.sum(:in_flight_count),
          partitions:     partitions.count,
          paused_count:   partitions.paused.count
        }
      end
    end

    def show
      @policy_object = DispatchPolicy.registry.fetch(@policy_name)
      @partitions    = Partition.for_policy(@policy_name)
                                .order(Arel.sql("pending_count DESC, last_admit_at DESC NULLS LAST"))
                                .limit(100)
      @totals = {
        pending:   Partition.for_policy(@policy_name).sum(:pending_count),
        in_flight: Partition.for_policy(@policy_name).sum(:in_flight_count),
        partitions: Partition.for_policy(@policy_name).count
      }
    end

    def pause
      Partition.for_policy(@policy_name).update_all(status: "paused", updated_at: Time.current)
      redirect_to policy_path(@policy_name), notice: "Policy paused."
    end

    def resume
      Partition.for_policy(@policy_name).update_all(status: "active", updated_at: Time.current)
      redirect_to policy_path(@policy_name), notice: "Policy resumed."
    end

    private

    def find_policy
      @policy_name = params[:name]
    end
  end
end
