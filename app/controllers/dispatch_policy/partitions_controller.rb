# frozen_string_literal: true

module DispatchPolicy
  class PartitionsController < ApplicationController
    before_action :find_partition, only: %i[show drain admit]

    DRAIN_MAX_PER_REQUEST = 10_000
    DRAIN_BATCH_SIZE      = 200

    PAGE_SIZE = 100
    SORT_OPTIONS = {
      "pending"    => "pending_count DESC, last_admit_at DESC NULLS LAST, id ASC",
      "admitted"   => "total_admitted DESC, id ASC",
      "stale"      => "last_checked_at ASC NULLS FIRST, id ASC",
      "recent"     => "last_admit_at DESC NULLS LAST, id ASC",
      "key"        => "partition_key ASC, id ASC"
    }.freeze
    DEFAULT_SORT = "pending"

    def index
      scope = Partition.all
      scope = scope.for_policy(params[:policy]) if params[:policy].present?
      scope = scope.for_shard(params[:shard])   if params[:shard].present?
      scope = scope.where("partition_key ILIKE ?", "%#{params[:q]}%") if params[:q].present?
      scope = scope.where("pending_count > 0")                         if params[:only_pending] == "1"

      @sort  = SORT_OPTIONS.key?(params[:sort]) ? params[:sort] : DEFAULT_SORT
      scope  = scope.order(Arel.sql(SORT_OPTIONS.fetch(@sort)))

      @page       = [Integer(params[:page] || 1), 1].max
      @page_size  = PAGE_SIZE
      @total      = scope.count
      @last_page  = [(@total.to_f / @page_size).ceil, 1].max
      @page       = [@page, @last_page].min
      @partitions = scope.offset((@page - 1) * @page_size).limit(@page_size)

      @policy        = params[:policy]
      @shard         = params[:shard]
      @query         = params[:q]
      @only_pending  = params[:only_pending] == "1"

      shards_scope = Partition.all
      shards_scope = shards_scope.for_policy(params[:policy]) if params[:policy].present?
      @shards      = shards_scope.distinct.pluck(:shard).sort
    end

    def pagination_params(overrides = {})
      {
        policy:        @policy.presence,
        shard:         @shard.presence,
        q:             @query.presence,
        sort:          (@sort if @sort != DEFAULT_SORT),
        only_pending:  ("1" if @only_pending),
        page:          (@page if @page > 1)
      }.compact.merge(overrides)
    end
    helper_method :pagination_params

    def show
      @recent_jobs = StagedJob
        .for_partition(@partition.policy_name, @partition.partition_key)
        .order(:scheduled_at, :id)
        .limit(50)
      @inflight = InflightJob.where(policy_name: @partition.policy_name).limit(50)
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

    # Empties the partition by force-admitting every staged job through the
    # forwarder, bypassing all gates. Bounded at DRAIN_MAX_PER_REQUEST so a
    # huge backlog can't time the controller out — the operator clicks again
    # for the next batch.
    def drain
      drained, remaining = self.class.drain_partition!(@partition)
      notice = if remaining.positive?
        "Drained #{drained} job(s); #{remaining} still pending — click drain again to continue."
      else
        "Drained #{drained} job(s); partition empty."
      end
      redirect_to partition_path(@partition), notice: notice
    end

    def self.drain_partition!(partition)
      drained = 0
      while drained < DRAIN_MAX_PER_REQUEST
        batch_limit = [DRAIN_BATCH_SIZE, DRAIN_MAX_PER_REQUEST - drained].min
        rows = Repository.claim_staged_jobs!(
          policy_name:      partition.policy_name,
          partition_key:    partition.partition_key,
          limit:            batch_limit,
          gate_state_patch: {},
          retry_after:      nil
        )
        break if rows.empty?

        Forwarder.dispatch(rows)
        drained += rows.size
      end
      remaining = partition.class.where(id: partition.id).pick(:pending_count) || 0
      [drained, remaining]
    end

    private

    def find_partition
      @partition = Partition.find(params[:id])
    end
  end
end
