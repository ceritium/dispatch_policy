# frozen_string_literal: true

module DispatchPolicy
  class PartitionsController < ApplicationController
    before_action :find_partition, only: %i[show drain admit requeue]

    DRAIN_MAX_PER_REQUEST = 10_000
    DRAIN_BATCH_SIZE      = 200

    PAGE_SIZE = 100

    def index
      base = Partition.all
      base = base.for_policy(params[:policy]) if params[:policy].present?
      base = base.for_shard(params[:shard])   if params[:shard].present?
      if params[:q].present?
        # Escape %/_ so a literal key containing them (e.g. "discount_50%")
        # matches literally instead of as ILIKE wildcards.
        base = base.where("partition_key ILIKE ?", "%#{Partition.sanitize_sql_like(params[:q])}%")
      end
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

      # Policy-level pause flags so rows show their EFFECTIVE state: a
      # partition created after a pause has status 'active' but is not
      # being admitted (claim_partitions skips the whole policy).
      @paused_policies = PolicySetting.paused.pluck(:policy_name).to_set

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
        .deliverable
        # Mirrors claim_staged_jobs! exactly (audit L9): the list is
        # "what comes out next", so the two orders must not drift — and
        # the claim skips quarantined rows, so this must too.
        .order(Arel.sql("priority ASC, scheduled_at ASC NULLS FIRST, id ASC"))
        .limit(50)
      # Listed separately because they are the opposite of "what comes out
      # next": nothing will ever admit them until someone acts.
      @quarantined_jobs = StagedJob
        .for_partition(@partition.policy_name, @partition.partition_key)
        .quarantined.order(failed_at: :desc).limit(50)
      # The whole policy may be paused even if this partition's own status
      # is 'active' (it was created after the pause). claim_partitions skips
      # the policy regardless, so surface the effective state.
      @policy_paused = PolicySetting.for_policy(@partition.policy_name).pick(:paused) || false
      # Backoff, round-trip age and the decay elapsed time all read
      # Postgres-written columns, so the database computes them rather than
      # the view subtracting them from Time.current. See
      # Repository#partition_clock_facts.
      @clock_facts = Repository.partition_clock_facts(
        policy_name: @partition.policy_name, partition_key: @partition.partition_key
      )
    end

    def admit
      # Bound the count: an unbounded value would force a single
      # DELETE…RETURNING + dispatch of the whole backlog in one transaction,
      # bypassing the batching/cap that #drain uses precisely to avoid
      # request timeouts and giant transactions. A non-numeric value falls
      # back to 1 instead of raising (ArgumentError → 500).
      count     = (Integer(params[:count], exception: false) || 1).clamp(1, DRAIN_MAX_PER_REQUEST)
      begin
        forwarded = ManualAdmission.force!(
          policy_name:   @partition.policy_name,
          partition_key: @partition.partition_key,
          limit:         count
        )
      rescue StandardError => e
        # #drain has had this isolation since the audit; admit did not, so
        # anything the forward raises — a job_class the web process cannot
        # resolve is the usual one — reached the operator as a bare 500.
        DispatchPolicy.config.logger&.error(
          "[dispatch_policy] admit failed for #{@partition.policy_name}/" \
          "#{@partition.partition_key}: #{e.class}: #{e.message}"
        )
        return redirect_to partition_path(@partition),
                           alert: "Could not forward: #{e.class} — see logs."
      end
      redirect_to partition_path(@partition), notice: "Forwarded #{forwarded} job(s)."
    end

    # Puts quarantined rows back in play. This is the only correct inverse
    # of the quarantine: clearing `failed_at` by hand leaves pending_count
    # where the quarantine left it, and `claim_partitions` requires
    # `pending_count > 0`, so the rows come back deliverable and no tick
    # ever claims them again.
    def requeue
      requeued = Repository.requeue_quarantined_jobs!(
        policy_name:   @partition.policy_name,
        partition_key: @partition.partition_key
      )
      redirect_to partition_path(@partition),
                  notice: "Requeued #{requeued} undeliverable job(s); the next tick will " \
                          "try them again."
    end

    # Empties the partition by force-admitting every staged job through the
    # forwarder, bypassing all gates. Bounded at DRAIN_MAX_PER_REQUEST so a
    # huge backlog can't time the controller out — the operator clicks again
    # for the next batch.
    def drain
      drained, due_remaining, scheduled_remaining, failed =
        self.class.drain_partition!(@partition)

      notice =
        if failed
          "Drained #{drained} job(s); this partition's next job could not be forwarded — see logs."
        elsif due_remaining.positive?
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
    # across partitions. Returns [drained, due_remaining, scheduled_remaining,
    # failed] — due_remaining is claimable-now work the cap left behind;
    # scheduled_remaining is future-scheduled rows the claim can't touch
    # yet; failed says a batch raised and this partition was abandoned.
    #
    # The rescue is the isolation `Tick#admit_partition` already gives the
    # automatic path, and the drain button needs it more: it is what an
    # operator reaches for precisely when something is wrong. One
    # undeserialisable row — a job class renamed or deleted in a deploy
    # while its staged rows are still around — made `Forwarder.dispatch`
    # raise NameError out of the controller as a bare 500: no flash, no
    # partition name, no count, nothing drained, and every healthy
    # partition behind it never reached, on every click. The raise itself
    # stays where it is; that is what rolls the claim TX back and saves
    # the staged rows. Break rather than continue, too: the poison row is
    # at the head of the queue, so retrying the same batch would spin.
    def self.drain_partition!(partition, cap: DRAIN_MAX_PER_REQUEST)
      cap     = [cap, DRAIN_MAX_PER_REQUEST].min
      drained = 0
      failed  = false
      while drained < cap
        batch_limit = [DRAIN_BATCH_SIZE, cap - drained].min
        begin
          forwarded = ManualAdmission.force!(
            policy_name:   partition.policy_name,
            partition_key: partition.partition_key,
            limit:         batch_limit
          )
        rescue StandardError => e
          DispatchPolicy.config.logger&.error(
            "[dispatch_policy] drain failed for " \
            "#{partition.policy_name}/#{partition.partition_key}: #{e.class}: #{e.message}"
          )
          failed = true
          break
        end
        break if forwarded.zero?

        drained += forwarded
      end

      scope               = StagedJob.for_partition(partition.policy_name, partition.partition_key)
                                     .deliverable
      due_remaining       = scope.due.count
      scheduled_remaining = scope.count - due_remaining
      [drained, due_remaining, scheduled_remaining, failed]
    end

    private

    def find_partition
      @partition = Partition.find(params[:id])
    end
  end
end
