# frozen_string_literal: true

module DispatchPolicy
  # One pass of admission for a single policy.
  #
  # Records a row in dispatch_policy_tick_samples at the end so the engine UI
  # can show throughput, denial reasons, and tick duration without sampling
  # on the read path.
  class Tick
    Result = Struct.new(:partitions_seen, :jobs_admitted, keyword_init: true)

    def self.run(policy_name:, shard: nil)
      new(policy_name, shard: shard).call
    end

    def initialize(policy_name, shard: nil)
      @policy_name = policy_name
      @shard       = shard
      @policy      = DispatchPolicy.registry.fetch(policy_name) || raise(InvalidPolicy, "unknown policy #{policy_name.inspect}")
      @config      = DispatchPolicy.config
    end

    def call
      started_at         = monotonic_now_ms
      partitions_seen    = 0
      partitions_admitted = 0
      partitions_denied   = 0
      jobs_admitted      = 0
      forward_failures   = 0
      denied_reasons     = Hash.new(0)

      partitions = Repository.claim_partitions(
        policy_name: @policy_name,
        shard:       @shard,
        limit:       @config.partition_batch_size
      )

      # Reorder by least-recent-admit-weighted (EWMA decayed_admits ASC)
      # so under-admitted partitions get first crack at the tick budget.
      # claim_partitions ALREADY enforced anti-stagnation via
      # last_checked_at — every partition with pending is visited within
      # ⌈active_partitions / partition_batch_size⌉ ticks regardless of
      # decayed_admits. Reordering here only decides order *inside* this
      # already-fair selection.
      sort_partitions_for_fairness!(partitions)

      # Per-partition fair share. When tick_admission_budget is set, we
      # divide it evenly across the partitions we just claimed. Otherwise
      # the legacy admission_batch_size is the per-partition ceiling.
      #
      # We deliberately do NOT clamp fair_share to a minimum of 1 when
      # tick_cap < N. The hard global cap wins over a per-partition
      # admit floor; partitions that don't admit this tick are still
      # visited (last_checked_at bumped) and re-visited next tick when
      # they'll be at the front of the in-tick decay order.
      # Anti-stagnation comes from claim_partitions, not from forcing
      # an admit on every claimed partition.
      tick_cap   = @policy.tick_admission_budget || @config.tick_admission_budget
      per_part   = @policy.admission_batch_size || @config.admission_batch_size
      fair_share = if tick_cap && partitions.any?
                     (tick_cap.to_f / partitions.size).ceil
                   else
                     per_part
                   end

      pending_denies          = []
      admitted_per_partition  = Hash.new(0)
      used                    = 0

      partitions.each do |partition|
        partitions_seen += 1

        if tick_cap && used >= tick_cap
          # Global cap exhausted in pass-1. The partition is still
          # observed (claim_partitions bumped its last_checked_at), so
          # the round-robin invariant for anti-stagnation holds; we
          # just admit nothing this tick.
          partitions_denied += 1
          denied_reasons["tick_cap_exhausted"] += 1
          # Push this partition to the deny path so its gate state
          # still gets persisted — the pipeline already evaluated it
          # in admit_partition... actually we haven't called admit yet.
          # Skip: not adding to pending_denies because the pipeline
          # didn't run, no gate_state_patch to flush.
          next
        end

        budget_for_this = if tick_cap
                            [fair_share, tick_cap - used].min
                          else
                            fair_share
                          end
        budget_for_this = 0 if budget_for_this.negative?

        outcome = admit_partition(partition, pending_denies, max_budget: budget_for_this)
        admitted_per_partition[partition["partition_key"]] = outcome[:admitted]

        jobs_admitted    += outcome[:admitted]
        forward_failures += outcome[:failures]
        used             += outcome[:admitted]

        if outcome[:admitted].positive?
          partitions_admitted += 1
        else
          partitions_denied += 1
          outcome[:reasons].each { |r| denied_reasons[r] += 1 }
        end
      end

      # Pass-2: redistribution. Pass-1 may have left budget unused if
      # some partitions had less pending than their fair share. Walk the
      # claimed partitions (still in decay-sorted order) and offer the
      # leftover to whoever filled their fair share in pass-1 — a signal
      # they had more pending than we let them admit.
      if tick_cap
        remaining = tick_cap - used
        if remaining.positive?
          partitions.each do |p|
            break if remaining <= 0
            next  if admitted_per_partition[p["partition_key"]] < fair_share

            extra_cap = [remaining, fair_share].min
            outcome   = admit_partition(p, pending_denies, max_budget: extra_cap)
            jobs_admitted += outcome[:admitted]
            forward_failures += outcome[:failures]
            admitted_per_partition[p["partition_key"]] += outcome[:admitted]
            remaining -= outcome[:admitted]
          end
        end
      end

      flush_denies!(pending_denies) if pending_denies.any?

      duration_ms = monotonic_now_ms - started_at

      record_sample!(
        duration_ms:         duration_ms,
        partitions_seen:     partitions_seen,
        partitions_admitted: partitions_admitted,
        partitions_denied:   partitions_denied,
        jobs_admitted:       jobs_admitted,
        forward_failures:    forward_failures,
        denied_reasons:      denied_reasons
      )

      Result.new(partitions_seen: partitions_seen, jobs_admitted: jobs_admitted)
    end

    private

    # In-place sort by current decayed_admits ASC, computed in Ruby from
    # the row's stored decayed_admits + the elapsed time since
    # decayed_admits_at. We do this here (rather than in the SQL of
    # claim_partitions) because:
    #
    # - claim_partitions's ORDER BY is anti-stagnation (last_checked_at
    #   NULLS FIRST); reordering there would bias selection itself,
    #   reintroducing the stagnation risk.
    # - The math is cheap on N ≤ partition_batch_size rows already in
    #   memory.
    def sort_partitions_for_fairness!(partitions)
      half_life = @policy.fairness_half_life_seconds || @config.fairness_half_life_seconds
      return partitions if half_life.nil? || half_life <= 0

      tau = half_life.to_f / Math.log(2)
      now = Time.current.to_f

      partitions.sort_by! do |p|
        last_t = decayed_admits_epoch(p["decayed_admits_at"]) || now
        elapsed = [now - last_t, 0.0].max
        (p["decayed_admits"] || 0.0).to_f * Math.exp(-elapsed / tau)
      end
    end

    def decayed_admits_epoch(value)
      return nil if value.nil?
      return value.to_f if value.is_a?(Numeric)
      return value.to_time.to_f if value.respond_to?(:to_time)
      Time.parse(value.to_s).to_f
    rescue ArgumentError, TypeError
      nil
    end

    def admit_partition(partition, pending_denies, max_budget:)
      ctx        = Context.wrap(partition["context"])
      pipe       = Pipeline.new(@policy)
      result     = pipe.call(ctx, partition, max_budget)

      # Pure-deny path (gate said no capacity for this partition this tick).
      # Defer the partition state UPDATE to the bulk flush at the end of
      # the tick instead of issuing a per-partition statement now.
      if result.admit_count.zero?
        pending_denies << {
          policy_name:      @policy_name,
          partition_key:    partition["partition_key"],
          gate_state_patch: result.gate_state_patch,
          retry_after:      result.retry_after
        }
        return { admitted: 0, failures: 0, reasons: deduce_reasons(result) }
      end

      admitted = 0
      half_life = @policy.fairness_half_life_seconds || @config.fairness_half_life_seconds

      Repository.with_connection do
        ActiveRecord::Base.transaction(requires_new: true) do
          rows = Repository.claim_staged_jobs!(
            policy_name:       @policy_name,
            partition_key:     partition["partition_key"],
            limit:             result.admit_count,
            gate_state_patch:  result.gate_state_patch,
            retry_after:       result.retry_after,
            half_life_seconds: half_life
          )

          # `claim_staged_jobs!` always runs `record_partition_admit!` so
          # the partition's counters and gate_state commit even when the
          # actual DELETE returned zero rows (e.g. all staged rows are
          # scheduled in the future, or another tick raced us to them).
          next if rows.empty?

          # Pre-insert an inflight row per admitted job so the concurrency
          # gate sees them immediately. With a concurrency gate, use its
          # (coarser) partition key so the gate's COUNT(*) keeps aggregating
          # correctly across staged sub-partitions.
          concurrency_gate = @policy.gates.find { |g| g.name == :concurrency }
          inflight_rows = rows.filter_map do |row|
            ajid = row.dig("job_data", "job_id")
            next unless ajid

            key = if concurrency_gate
              concurrency_gate.inflight_partition_key(@policy_name, Context.wrap(row["context"]))
            else
              row["partition_key"]
            end
            { policy_name: @policy_name, partition_key: key, active_job_id: ajid }
          end
          Repository.insert_inflight!(inflight_rows) if inflight_rows.any?

          # Re-enqueue to the real adapter *inside this transaction*. The
          # adapter (good_job / solid_queue) shares ActiveRecord::Base's
          # connection, so its INSERT into good_jobs / solid_queue_jobs
          # participates in the same TX. If anything raises (deserialize,
          # adapter error, network), the whole TX rolls back atomically:
          # staged_jobs return, inflight rows vanish, partition counters
          # revert, and the adapter rows are also reverted. This is the
          # at-least-once guarantee — there is no window where staged is
          # gone but the adapter never received the job.
          Forwarder.dispatch(rows)
          admitted = rows.size
        end
      end

      if admitted.zero?
        { admitted: 0, failures: 0, reasons: ["no_rows_claimed"] }
      else
        { admitted: admitted, failures: 0, reasons: [] }
      end
    rescue StandardError => e
      DispatchPolicy.config.logger&.error(
        "[dispatch_policy] forward failed for #{@policy_name}/#{partition['partition_key']}: " \
        "#{e.class}: #{e.message}"
      )
      { admitted: 0, failures: 1, reasons: ["forward_failed"] }
    end

    def flush_denies!(entries)
      Repository.with_connection { Repository.bulk_record_partition_denies!(entries) }
    rescue StandardError => e
      DispatchPolicy.config.logger&.error(
        "[dispatch_policy] bulk_record_partition_denies failed: #{e.class}: #{e.message}"
      )
    end

    # When admit_count was 0, the Pipeline's `reasons` array contains entries
    # like "throttle:rate=0", "concurrency:concurrency_full". We strip the
    # `gate:` prefix's value separator so callers see "throttle" / "concurrency_full".
    def deduce_reasons(result)
      reasons = result.reasons.map do |s|
        gate, msg = s.split(":", 2)
        msg.presence || gate
      end
      reasons << "no_capacity" if reasons.empty?
      reasons
    end

    def record_sample!(**fields)
      pending_total  = DispatchPolicy::Partition.for_policy(@policy_name).sum(:pending_count)
      inflight_total = DispatchPolicy::InflightJob.where(policy_name: @policy_name).count

      Repository.record_tick_sample!(
        policy_name:    @policy_name,
        pending_total:  pending_total,
        inflight_total: inflight_total,
        **fields
      )
    rescue StandardError => e
      DispatchPolicy.config.logger&.warn("[dispatch_policy] failed to record tick sample: #{e.class}: #{e.message}")
    end

    def monotonic_now_ms
      (Process.clock_gettime(Process::CLOCK_MONOTONIC) * 1000).to_i
    end
  end
end
