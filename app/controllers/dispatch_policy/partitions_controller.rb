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
      # Order matches the tick's claim order (claim_staged_jobs!) so the list
      # reflects what would actually be admitted first, not the reverse.
      @recent_jobs = StagedJob
        .for_partition(@partition.policy_name, @partition.partition_key)
        .order(Arel.sql("priority DESC, scheduled_at ASC NULLS FIRST, id ASC"))
        .limit(50)
    end

    def admit
      # Bound the count: an unbounded value would force a single
      # DELETE…RETURNING + dispatch of the whole backlog in one transaction,
      # bypassing the batching/cap that #drain uses precisely to avoid
      # request timeouts and giant transactions. A non-numeric value falls
      # back to 1 instead of raising (ArgumentError → 500).
      count     = (Integer(params[:count], exception: false) || 1).clamp(1, DRAIN_MAX_PER_REQUEST)
      forwarded = ManualAdmission.force!(
        policy_name:   @partition.policy_name,
        partition_key: @partition.partition_key,
        limit:         count
      )
      redirect_to partition_path(@partition), notice: "Forwarded #{forwarded} job(s)."
    end

    # Empties the partition by force-admitting every staged job through the
    # forwarder, bypassing all gates. Bounded at DRAIN_MAX_PER_REQUEST so a
    # huge backlog can't time the controller out — the operator clicks again
    # for the next batch.
    def drain
      drained, due_remaining, scheduled_remaining =
        self.class.drain_partition!(@partition)

      notice =
        if due_remaining.positive?
          "Drained #{drained} job(s); #{due_remaining} still pending — click drain again to continue."
        elsif scheduled_remaining.positive?
          # The claim only picks up rows whose scheduled_at has arrived, so
          # future-scheduled jobs can't be drained now. Saying "click again"
          # would just loop forwarding zero.
          "Drained #{drained} job(s); #{scheduled_remaining} scheduled for later remain."
        else
          "Drained #{drained} job(s); partition empty."
        end
      redirect_to partition_path(@partition), notice: notice
    end

    # Force-admits up to DRAIN_MAX_PER_REQUEST due jobs in DRAIN_BATCH_SIZE
    # batches. Optional `cap` lets the policy-wide drain bound the TOTAL
    # across partitions. Returns [drained, due_remaining, scheduled_remaining]
    # — due_remaining is claimable-now work the cap left behind;
    # scheduled_remaining is future-scheduled rows the claim can't touch yet.
    def self.drain_partition!(partition, cap: DRAIN_MAX_PER_REQUEST)
      cap     = [cap, DRAIN_MAX_PER_REQUEST].min
      drained = 0
      while drained < cap
        batch_limit = [DRAIN_BATCH_SIZE, cap - drained].min
        forwarded   = ManualAdmission.force!(
          policy_name:   partition.policy_name,
          partition_key: partition.partition_key,
          limit:         batch_limit
        )
        break if forwarded.zero?

        drained += forwarded
      end

      scope               = StagedJob.for_partition(partition.policy_name, partition.partition_key)
      due_remaining       = scope.due.count
      scheduled_remaining = scope.count - due_remaining
      [drained, due_remaining, scheduled_remaining]
    end

    private

    def find_partition
      @partition = Partition.find(params[:id])
    end
  end
end
