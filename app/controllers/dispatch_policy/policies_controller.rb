# frozen_string_literal: true

module DispatchPolicy
  class PoliciesController < ApplicationController
    PARTITION_LIST_PAGE_SIZE = 25

    before_action :reload_policies_from_db
    before_action :load_policy, only: :show

    def index
      @policies = DispatchPolicy.registry.map do |name, job_class|
        scope = StagedJob.where(policy_name: name)
        partitions = PolicyPartition.where(policy_name: name)
        {
          name:           name,
          job_class:      job_class,
          policy:         job_class.resolved_dispatch_policy,
          pending_count:  scope.pending.count,
          admitted_count: scope.admitted.count,
          completed_24h:  scope.completed.where(completed_at: 24.hours.ago..).count,
          partitions:     partitions.count,
          ready_partitions: partitions.where("pending_count > 0").count
        }
      end.sort_by { |p| -p[:pending_count] }

      @expired_leases   = StagedJob.expired_leases.count
      @tick_perf_window = tick_perf_window
      @tick_perf        = Stats.tick_runs(window: @tick_perf_window)

      @health = Stats.health
      @slos   = DispatchPolicy.registry.keys.map do |name|
        slo = Stats.slo(name)
        slo.merge(policy_name: name, bottleneck: Stats.bottleneck(name))
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

      @partition_search = params[:q].to_s.strip
      @partition_page   = [ params[:page].to_i, 1 ].max
      @partition_sort   = %w[partition pending in_flight tokens last_checked_at status].include?(params[:sort]) ? params[:sort] : "pending"
      @partition_dir    = params[:dir] == "asc" ? "asc" : "desc"

      list = build_partition_list(@policy_name)
      list = list.select { |r| r[:partition_key].downcase.include?(@partition_search.downcase) } if @partition_search.present?
      list = sort_partition_list(list, @partition_sort, @partition_dir)

      @partition_total_list = list.size
      offset                = (@partition_page - 1) * PARTITION_LIST_PAGE_SIZE
      @partition_list       = list[offset, PARTITION_LIST_PAGE_SIZE] || []

      @pending_jobs = scope.pending
        .select(:id, :partition_key, :dedupe_key, :priority, :staged_at, :not_before_at)
        .order(:priority, :staged_at)
        .limit(50)

      @tick_perf_window = tick_perf_window
      @tick_perf        = Stats.tick_runs(window: @tick_perf_window)
      @policy_config    = build_policy_configs(@policy_name).first
    end

    private

    # Reload each registered policy's @override_X from
    # dispatch_policy_policy_configs so the admin paints what the
    # next TickLoop will use, not whatever was loaded at boot.
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

    def build_partition_list(policy_name)
      cap = DispatchPolicy.config.admin_partition_limit
      now = Time.current

      PolicyPartition.where(policy_name: policy_name)
        .order(Arel.sql("(pending_count > 0) DESC, last_checked_at NULLS FIRST"))
        .limit(cap)
        .pluck(:partition_key, :pending_count, :in_flight, :concurrency_max,
               :tokens, :throttle_rate, :throttle_burst, :refilled_at,
               :last_checked_at)
        .map do |pk, pending, inflight, cmax, tok, rate, burst, refilled, last|
          eff_tokens = rate ? effective_tokens(tok, rate, burst, refilled, now) : nil
          status =
            if pending.zero? && inflight.zero?              then "idle"
            elsif cmax && inflight >= cmax                  then "concurrency_blocked"
            elsif rate && eff_tokens < 1                    then "throttle_blocked"
            else                                                "ready"
            end
          {
            partition_key:   pk,
            pending:         pending,
            in_flight:       inflight,
            tokens:          eff_tokens && eff_tokens.round(2),
            burst:           burst,
            status:          status,
            last_checked_at: last
          }
        end
    end

    def effective_tokens(tokens, rate, burst, refilled_at, now)
      return Float::INFINITY if rate.nil?
      return 0.0 if tokens.nil? || refilled_at.nil?
      elapsed = [ (now - refilled_at), 0 ].max
      [ burst.to_f, tokens.to_f + elapsed * rate.to_f ].min
    end

    def sort_partition_list(list, sort, dir)
      key =
        case sort
        when "partition"        then ->(r) { r[:partition_key] }
        when "pending"          then ->(r) { r[:pending] }
        when "in_flight"        then ->(r) { r[:in_flight] }
        when "tokens"           then ->(r) { r[:tokens] || -1 }
        when "last_checked_at"  then ->(r) { r[:last_checked_at]&.to_f || 0 }
        when "status"           then ->(r) { r[:status] }
        else                          ->(r) { r[:pending] }
        end
      sorted = list.sort_by(&key)
      dir == "asc" ? sorted : sorted.reverse
    end

    def build_policy_configs(only_name = nil)
      keys  = PolicyConfig::KNOWN_KEYS
      names = only_name ? [ only_name ] : DispatchPolicy.registry.keys

      db_rows = PolicyConfig.where(policy_name: names)
        .pluck(:policy_name, :config_key, :value, :source)
        .each_with_object({}) { |(pn, k, v, s), h| h[[ pn, k ]] = { value: v, source: s } }

      names.map do |name|
        policy = DispatchPolicy.registry[name]&.resolved_dispatch_policy
        next unless policy

        rows = keys.map do |key|
          db = db_rows[[ name, key.to_s ]]
          override = policy.config_overrides[key]
          source =
            if db
              db[:source]
            elsif override
              "code (in-memory)"
            else
              "default"
            end

          value = policy.respond_to?("effective_#{key}") ? policy.public_send("effective_#{key}") : nil
          {
            key:      key,
            value:    value,
            source:   source,
            db_value: db && db[:value]
          }
        end

        {
          policy_name:      name,
          config:           rows,
          auto_tune:        policy.effective_auto_tune,
          auto_tune_source: policy.instance_variable_get(:@override_auto_tune).nil? ? "global" : "policy"
        }
      end.compact
    end

    def tick_perf_window
      raw = params[:window].to_i
      raw = 300 if raw <= 0
      raw.clamp(60, 86_400)
    end

    def load_policy
      @policy_name = params[:policy_name]
      @job_class   = DispatchPolicy.registry[@policy_name] ||
                     Dispatch.autoload_job_for(@policy_name)
      raise ActiveRecord::RecordNotFound unless @job_class
      @policy = @job_class.resolved_dispatch_policy
    end
  end
end
