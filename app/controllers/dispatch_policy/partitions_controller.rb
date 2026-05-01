# frozen_string_literal: true

module DispatchPolicy
  class PartitionsController < ApplicationController
    before_action :find_partition, only: %i[show clear admit]

    def index
      scope = Partition.all
      scope = scope.for_policy(params[:policy]) if params[:policy].present?
      if (q = params[:q]).present?
        scope = scope.where("partition_key ILIKE ?", "%#{q}%")
      end
      scope = scope.order(Arel.sql("pending_count DESC, last_admit_at DESC NULLS LAST"))

      @policy     = params[:policy]
      @query      = params[:q]
      @partitions = scope.limit(200)
    end

    def show
      @recent_jobs = StagedJob
        .for_partition(@partition.policy_name, @partition.partition_key)
        .order(:scheduled_at, :id)
        .limit(50)
      @inflight = InflightJob.where(policy_name: @partition.policy_name).limit(50)
    end

    def clear
      DispatchPolicy::ApplicationRecord.transaction do
        StagedJob.for_partition(@partition.policy_name, @partition.partition_key).delete_all
        @partition.update!(pending_count: 0)
      end
      redirect_to partition_path(@partition), notice: "Partition cleared."
    end

    def admit
      count = Integer(params[:count] || 1)
      rows = Repository.claim_staged_jobs!(
        policy_name:      @partition.policy_name,
        partition_key:    @partition.partition_key,
        limit:            count,
        gate_state_patch: {},
        retry_after:      nil
      )
      forwarded = rows.size - Forwarder.dispatch(rows).size
      redirect_to partition_path(@partition), notice: "Forwarded #{forwarded} job(s)."
    end

    private

    def find_partition
      @partition = Partition.find(params[:id])
    end
  end
end
