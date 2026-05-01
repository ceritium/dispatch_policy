# frozen_string_literal: true

module DispatchPolicy
  class PartitionsController < ApplicationController
    before_action :find_partition, only: %i[show drain admit]

    DRAIN_MAX_PER_REQUEST = 10_000
    DRAIN_BATCH_SIZE      = 200

    PAGE_SIZE = 100

    def index
      base = Partition.all
      base = base.for_policy(params[:policy]) if params[:policy].present?
      base = base.for_shard(params[:shard])   if params[:shard].present?
      base = base.where("partition_key ILIKE ?", "%#{params[:q]}%") if params[:q].present?
      base = base.where("pending_count > 0")                         if params[:only_pending] == "1"

      @sort = DispatchPolicy::CursorPagination::SORTS.key?(params[:sort]) ? params[:sort] : DispatchPolicy::CursorPagination::DEFAULT_SORT
      sort_def = DispatchPolicy::CursorPagination.sort_for(@sort)

      @total  = base.count   # cheap on indexed columns; nice to display
      @cursor = DispatchPolicy::CursorPagination.decode(params[:cursor])

      paginated = DispatchPolicy::CursorPagination.apply(base, @sort, @cursor)
                                                   .order(Arel.sql(sort_def[:sql_order]))
                                                   .limit(PAGE_SIZE + 1)
                                                   .to_a

      @has_more   = paginated.size > PAGE_SIZE
      @partitions = paginated.first(PAGE_SIZE)
      @next_cursor =
        if @has_more && @partitions.any?
          v, id = DispatchPolicy::CursorPagination.extract(@partitions.last, @sort)
          DispatchPolicy::CursorPagination.encode(v, id)
        end

      @policy        = params[:policy]
      @shard         = params[:shard]
      @query         = params[:q]
      @only_pending  = params[:only_pending] == "1"

      shards_scope = Partition.all
      shards_scope = shards_scope.for_policy(params[:policy]) if params[:policy].present?
      @shards      = shards_scope.distinct.pluck(:shard).sort
    end

    # Build URL params preserving filters, replacing the cursor.
    def pagination_params(overrides = {})
      {
        policy:        @policy.presence,
        shard:         @shard.presence,
        q:             @query.presence,
        sort:          (@sort if @sort != DispatchPolicy::CursorPagination::DEFAULT_SORT),
        only_pending:  ("1" if @only_pending),
        cursor:        nil
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
