# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    STALE_PENDING_THRESHOLD = 1.hour
    PARTITION_LIST_PAGE_SIZE = 25

    before_action :reload_policies_from_db
    before_action :load_policy, only: :show

    def index
      @policies = DispatchPolicy.registry.map do |name, job_class|
        scope   = StagedJob.where(policy_name: name)
        pending = scope.pending
        {
          name:            name,
          job_class:       job_class,
          policy:          job_class.resolved_dispatch_policy,
          pending_count:   pending.count,
          admitted_count:  scope.admitted.count,
          completed_24h:   scope.completed.where(completed_at: 24.hours.ago..).count,
          oldest_pending:  pending.minimum(:staged_at),
          stale_threshold: STALE_PENDING_THRESHOLD
        }
      end.sort_by { |p| -p[:pending_count] }

      @active_partitions = PartitionInflightCount.where("in_flight > 0").count
      @expired_leases    = StagedJob.expired_leases.count
      @tick_perf_window  = tick_perf_window
      @tick_perf         = Stats.tick_runs(window: @tick_perf_window)

      # Live SLO snapshot per policy: latency, fairness (LRU vs
      # gate-blocked split), capacity utilization, plus the
      # bottleneck diagnosis and concrete recommended_config so the
      # operator can see what to tune at a glance.
      @health = Stats.health
      @slos   = DispatchPolicy.registry.keys.map do |name|
        slo = Stats.slo(name)
        slo.merge(
          policy_name: name,
          bottleneck:  Stats.bottleneck(name)
        )
      end

      @policy_configs = build_policy_configs
    end

    def show
      scope = StagedJob.where(policy_name: @policy_name)
      @pending_count           = scope.pending.count
      @pending_eligible_count  = scope.pending.where("not_before_at IS NULL OR not_before_at <= ?", Time.current).count
      @pending_scheduled_count = @pending_count - @pending_eligible_count
      @admitted_count          = scope.admitted.count
      @completed_24h           = scope.completed.where(completed_at: 24.hours.ago..).count

      all_breakdown = partition_breakdown(scope)

      # "Watched" subset (passed via ?watch=a,b,c; the JS layer syncs it
      # with localStorage so the choice sticks across reloads).
      @watched_keys        = (params[:watch] || "").split(",").map(&:strip).reject(&:empty?)
      @partition_breakdown = @watched_keys.any? ? all_breakdown.select { |r| @watched_keys.include?(r[:partition]) } : []

      # Browsable list of every active partition with filter + sort + pagination.
      @partition_search = params[:q].to_s.strip
      @partition_page   = [ params[:page].to_i, 1 ].max
      @partition_sort   = %w[source partition pending in_flight completed_24h last_enqueued_at last_dispatched_at].include?(params[:sort]) ? params[:sort] : "activity"
      @partition_dir    = params[:dir] == "asc" ? "asc" : "desc"

      list = all_breakdown
      list = list.select { |r| r[:partition].to_s.downcase.include?(@partition_search.downcase) } if @partition_search.present?
      list = sort_partition_list(list, @partition_sort, @partition_dir)

      @partition_total_list = list.size
      offset                = (@partition_page - 1) * PARTITION_LIST_PAGE_SIZE
      @partition_list       = list[offset, PARTITION_LIST_PAGE_SIZE] || []

      load_adaptive_chart_data
      @throttle_buckets = ThrottleBucket
        .where(policy_name: @policy_name).order(:gate_name, :partition_key).limit(50)
      # Explicit select: don't load the `arguments` jsonb (job payload —
      # may contain PII / tokens) into memory just to render six fields.
      @pending_jobs = scope.pending
        .select(:id, :dedupe_key, :round_robin_key, :priority, :staged_at, :not_before_at)
        .order(:priority, :staged_at)
        .limit(50)

      # TickLoop normally runs in "all-policies" mode (one tick admits
      # from every policy in a single TX), so per-policy filtering
      # would return 0 samples even though the loop is healthy. Show
      # the aggregate — it's the engine's perf, not the policy's, and
      # that's still the right diagnostic on a per-policy page.
      @tick_perf_window = tick_perf_window
      @tick_perf        = Stats.tick_runs(window: @tick_perf_window)

      @policy_config = build_policy_configs(@policy_name).first
    end

    private

    # Each process (Puma vs GoodJob worker) has its own in-memory
    # @override_* on Policy. Without this, the admin shows whatever
    # config was loaded at boot — stale relative to what auto_tune
    # / the UI may have written from another process. Reloading
    # before the request paints from the same source of truth that
    # the next TickLoop spawn will use.
    def reload_policies_from_db
      DispatchPolicy.registry.each_value do |job_class|
        policy = job_class.resolved_dispatch_policy
        policy&.reload_overrides_from_db!
      end
    rescue ActiveRecord::StatementInvalid,
           ActiveRecord::ConnectionNotEstablished,
           ActiveRecord::NoDatabaseError
      # Migration not run yet — show what's in memory.
    end

    # Build a per-policy snapshot of effective config + source-of-
    # value (default / code DSL / DB row) + auto_tune mode. Used by
    # the Config panel on the index and show pages.
    def build_policy_configs(only_name = nil)
      keys  = PolicyConfig::KNOWN_KEYS
      names = only_name ? [ only_name ] : DispatchPolicy.registry.keys

      # One query for all DB-stored sources so the index doesn't issue
      # N round-trips when there are many policies.
      db_rows = PolicyConfig.where(policy_name: names)
        .pluck(:policy_name, :config_key, :value, :source)
        .each_with_object({}) { |(pn, k, v, s), h| h[[ pn, k ]] = { value: v, source: s } }

      names.map do |name|
        job_class = DispatchPolicy.registry[name]
        policy    = job_class&.resolved_dispatch_policy
        next unless policy

        rows = keys.map do |key|
          db    = db_rows[[ name, key.to_s ]]
          override = policy.config_overrides[key]
          # Source precedence:
          # - "auto" / "ui" / "code" come from the DB row's source field
          # - "code (in-memory)" if there's a Ruby override but no DB row
          #   (policy_config_source = :db hasn't run reload yet)
          # - "default" if neither override nor DB row exist
          source =
            if db
              db[:source]
            elsif override
              "code (in-memory)"
            else
              "default"
            end
          {
            key:     key,
            value:   policy.public_send("effective_#{key}"),
            source:  source,
            db_value: db && db[:value]
          }
        end

        {
          policy_name: name,
          config:      rows,
          auto_tune:   policy.effective_auto_tune,
          # Distinguish "policy declared it" from "fell back to global"
          auto_tune_source: policy.instance_variable_get(:@override_auto_tune).nil? ? "global" : "policy"
        }
      end.compact
    end

    # Window in seconds for the TickLoop perf widget. Configurable via
    # ?window= so an operator investigating a regression can widen
    # the lens (e.g. 3600 = last hour) without leaving the page.
    # Clamped to 60..86_400 so a malformed value can't crash the
    # query planner with an absurd time range.
    def tick_perf_window
      raw = params[:window].to_i
      raw = 300 if raw <= 0
      raw.clamp(60, 86_400)
    end

    def load_policy
      @policy_name = params[:policy_name]
      @job_class   = DispatchPolicy.registry[@policy_name] ||
                     Tick.autoload_job_for(@policy_name)
      raise ActiveRecord::RecordNotFound unless @job_class
      @policy = @job_class.resolved_dispatch_policy
    end

    # Per-(source, partition) breakdown of pending-eligible / pending-scheduled
    # / in-flight / completed-24h. A "source" is either a gate with a
    # partition_by (uses gate.partition_key_for(context)) or the policy's
    # round_robin_by declaration (uses the round_robin_key column directly).
    # All four counts come from StagedJob groupings; PartitionInflightCount
    # is an admission-time optimization, not the user-facing truth.
    def partition_breakdown(scope)
      sources = partition_sources
      return [] if sources.empty?

      now       = Time.current
      now_iso   = now.iso8601
      since_24h = 24.hours.ago.iso8601
      limit     = DispatchPolicy.config.admin_partition_limit
      @partition_breakdown_truncated = false

      adaptive_stats = AdaptiveConcurrencyStats.where(policy_name: @policy_name)
        .order(updated_at: :desc)
        .limit(limit)
        .pluck(:gate_name, :partition_key, :current_max, :ewma_latency_ms)
        .each_with_object({}) { |(g, k, c, l), h|
          h[[ g, k ]] = { current_max: c, ewma_latency_ms: l.to_f.round(1) }
        }

      rows = Hash.new { |h, k|
        h[k] = {
          source:             k[0],
          partition:          k[1],
          eligible:           0,
          scheduled:          0,
          in_flight:          0,
          completed_24h:      0,
          last_enqueued_at:   nil,
          last_dispatched_at: nil,
          current_max:        nil,
          ewma_latency_ms:    nil
        }
      }

      # Each aggregation below is order-by-count + limited so that a
      # policy with tens of thousands of distinct (context, round_robin_key)
      # tuples can't pull megabytes of rows into memory per request. We
      # show the top-N most-active partitions per axis and flip the
      # truncation flag for the view banner.

      # Activity timestamps bounded to the last 24h so the scan stays on
      # an index-friendly slice of staged_jobs.
      activity_rows = scope
        .where("staged_at > ?", since_24h)
        .group(:context, :round_robin_key)
        .order(Arel.sql("MAX(staged_at) DESC"))
        .limit(limit)
        .pluck(
          :context,
          :round_robin_key,
          Arel.sql("MAX(staged_at)"),
          Arel.sql("MAX(admitted_at)")
        )
      @partition_breakdown_truncated = true if activity_rows.size >= limit

      sources.each do |name, extract|
        pending_counts = scope.pending.group(:context, :round_robin_key)
          .order(Arel.sql("count(*) DESC"))
          .limit(limit)
          .pluck(
            :context,
            :round_robin_key,
            Arel.sql("count(*) filter (where not_before_at is null or not_before_at <= '#{now_iso}')"),
            Arel.sql("count(*) filter (where not_before_at > '#{now_iso}')")
          )
        @partition_breakdown_truncated = true if pending_counts.size >= limit
        pending_counts.each do |ctx, rr_key, eligible, scheduled|
          partition = extract.call(ctx, rr_key)
          row = rows[[ name, partition ]]
          row[:eligible]  += eligible
          row[:scheduled] += scheduled
        end

        admitted_counts = scope.admitted.group(:context, :round_robin_key)
          .order(Arel.sql("count(*) DESC"))
          .limit(limit)
          .pluck(:context, :round_robin_key, Arel.sql("count(*)"))
        @partition_breakdown_truncated = true if admitted_counts.size >= limit
        admitted_counts.each do |ctx, rr_key, in_flight|
          partition = extract.call(ctx, rr_key)
          rows[[ name, partition ]][:in_flight] += in_flight
        end

        completed_counts = scope.completed.where("completed_at > ?", since_24h)
          .group(:context, :round_robin_key)
          .order(Arel.sql("count(*) DESC"))
          .limit(limit)
          .pluck(:context, :round_robin_key, Arel.sql("count(*)"))
        @partition_breakdown_truncated = true if completed_counts.size >= limit
        completed_counts.each do |ctx, rr_key, completed|
          partition = extract.call(ctx, rr_key)
          rows[[ name, partition ]][:completed_24h] += completed
        end

        activity_rows.each do |ctx, rr_key, last_staged, last_admitted|
          partition = extract.call(ctx, rr_key)
          row       = rows[[ name, partition ]]
          row[:last_enqueued_at]   = [ row[:last_enqueued_at], last_staged ].compact.max
          row[:last_dispatched_at] = [ row[:last_dispatched_at], last_admitted ].compact.max
        end
      end

      rows.each do |(source, partition), row|
        stats = adaptive_stats[[ source, partition ]]
        next unless stats
        row[:current_max]     = stats[:current_max]
        row[:ewma_latency_ms] = stats[:ewma_latency_ms]
      end

      # Two different sources (say round_robin_by account_id + a gate
      # partitioned by account_id) producing the same partition key yield
      # identical counts — collapse them into one row with a merged source
      # label instead of listing the same numbers twice.
      merged = rows.values
        .reject { |r| r[:partition].nil? || r[:partition].empty? }
        .group_by { |r| [ r[:partition], r[:eligible], r[:scheduled], r[:in_flight], r[:completed_24h] ] }
        .map { |_, group|
          base = group.first.dup
          base[:source] = group.map { |r| r[:source] }.uniq.sort.join(" + ")
          group.each do |r|
            base[:current_max]        ||= r[:current_max]
            base[:ewma_latency_ms]    ||= r[:ewma_latency_ms]
            base[:last_enqueued_at]     = [ base[:last_enqueued_at], r[:last_enqueued_at] ].compact.max
            base[:last_dispatched_at]   = [ base[:last_dispatched_at], r[:last_dispatched_at] ].compact.max
          end
          base
        }

      merged.sort_by { |r|
        [ -(r[:eligible] + r[:scheduled] + r[:in_flight] + r[:completed_24h]), r[:source], r[:partition] ]
      }
    end

    def sort_partition_list(list, sort, dir)
      # Put nulls at the bottom regardless of direction (Time#to_f on nil
      # would crash; -Float::INFINITY sorts first, +Float::INFINITY last).
      key =
        case sort
        when "source"             then ->(r) { [ r[:source], r[:partition] ] }
        when "partition"          then ->(r) { r[:partition] }
        when "pending"            then ->(r) { r[:eligible] + r[:scheduled] }
        when "in_flight"          then ->(r) { r[:in_flight] }
        when "completed_24h"      then ->(r) { r[:completed_24h] }
        when "last_enqueued_at"   then ->(r) { r[:last_enqueued_at]&.to_f || 0 }
        when "last_dispatched_at" then ->(r) { r[:last_dispatched_at]&.to_f || 0 }
        else ->(r) { r[:eligible] + r[:scheduled] + r[:in_flight] + r[:completed_24h] }
        end
      sorted = list.sort_by(&key)
      dir == "asc" ? sorted : sorted.reverse
    end

    # Returns [[source_name, ->(ctx, rr_key) { partition_key }], ...]
    # covering every partition-producing declaration on the policy: every
    # gate with a partition_by, plus round_robin_by if declared.
    def partition_sources
      return [] unless @policy

      sources = @policy.gates.select(&:partition_by).map do |gate|
        [ gate.name.to_s, ->(ctx, _rr) { gate.partition_key_for((ctx || {}).symbolize_keys) } ]
      end
      sources << [ "round_robin_by", ->(_ctx, rr) { rr } ] if @policy.round_robin?
      sources
    end

    # Build chart data from PartitionObservation. Two queries:
    # - Global aggregated (one row per minute): cheap even with 1000s of
    #   partitions because we SUM/AVG in SQL, not in Ruby.
    # - Per-partition sparkline data, scoped to only the partitions we're
    #   going to actually render (breakdown's top N).
    def load_adaptive_chart_data
      last_minute   = Time.current.utc.beginning_of_minute
      @chart_slots  = (0..59).map { |i| last_minute - (59 - i).minutes }
      @chart_labels = @chart_slots.map { |t| t.strftime("%H:%M") }
      slot_index    = @chart_slots.each_with_index.to_h

      @adaptive_global    = Array.new(@chart_slots.size)
      @completions_global = Array.new(@chart_slots.size, 0)
      global_rows = PartitionObservation
        .where(policy_name: @policy_name)
        .where("minute_bucket >= ?", @chart_slots.first)
        .group(:minute_bucket)
        .pluck(:minute_bucket, Arel.sql("SUM(total_lag_ms)"), Arel.sql("SUM(observation_count)"))
      global_rows.each do |bucket, total_lag, obs_count|
        idx = slot_index[bucket.utc.beginning_of_minute]
        next unless idx
        @completions_global[idx] = obs_count
        @adaptive_global[idx]    = obs_count.positive? ? (total_lag.to_f / obs_count).round(1) : nil
      end

      partition_keys = (@partition_breakdown || []).map { |r| r[:partition] }.uniq
      @adaptive_samples    = {}
      @completions_samples = {}
      return if partition_keys.empty?

      per_partition_lag    = Hash.new { |h, k| h[k] = Array.new(@chart_slots.size) }
      per_partition_counts = Hash.new { |h, k| h[k] = Array.new(@chart_slots.size, 0) }
      rows = PartitionObservation
        .where(policy_name: @policy_name, partition_key: partition_keys)
        .where("minute_bucket >= ?", @chart_slots.first)
        .pluck(:partition_key, :minute_bucket, :total_lag_ms, :observation_count)
      rows.each do |pk, bucket, total, count|
        idx = slot_index[bucket.utc.beginning_of_minute]
        next unless idx
        per_partition_lag[pk][idx]    = count.positive? ? (total.to_f / count).round(1) : nil
        per_partition_counts[pk][idx] = count
      end
      @adaptive_samples    = per_partition_lag
      @completions_samples = per_partition_counts
    end
  end
end
