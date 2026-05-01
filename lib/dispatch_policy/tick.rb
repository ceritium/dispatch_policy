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

      partitions.each do |partition|
        partitions_seen += 1
        outcome = admit_partition(partition)

        jobs_admitted    += outcome[:admitted]
        forward_failures += outcome[:failures]

        if outcome[:admitted].positive?
          partitions_admitted += 1
        else
          partitions_denied += 1
          outcome[:reasons].each { |r| denied_reasons[r] += 1 }
        end
      end

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

    def admit_partition(partition)
      ctx        = Context.wrap(partition["context"])
      pipe       = Pipeline.new(@policy)
      max_budget = @policy.admission_batch_size || @config.admission_batch_size
      result     = pipe.call(ctx, partition, max_budget)

      preinserted_ids = []

      claimed_rows =
        ActiveRecord::Base.transaction(requires_new: true) do
          rows = Repository.claim_staged_jobs!(
            policy_name:      @policy_name,
            partition_key:    partition["partition_key"],
            limit:            result.admit_count,
            gate_state_patch: result.gate_state_patch,
            retry_after:      result.retry_after
          )

          if rows.any?
            # Always pre-insert an inflight row per admitted job so the UI
            # has an accurate count regardless of gates. If a concurrency
            # gate exists, use its (coarser) partition key so the gate's
            # query keeps aggregating correctly across staged partitions.
            concurrency_gate = @policy.gates.find { |g| g.name == :concurrency }
            inflight_rows = rows.filter_map do |row|
              ajid = row.dig("job_data", "job_id")
              next unless ajid

              key = if concurrency_gate
                concurrency_gate.inflight_partition_key(@policy_name, Context.wrap(row["context"]))
              else
                row["partition_key"]
              end

              preinserted_ids << ajid
              { policy_name: @policy_name, partition_key: key, active_job_id: ajid }
            end
            Repository.insert_inflight!(inflight_rows) if inflight_rows.any?
          end

          rows
        end

      if claimed_rows.empty?
        return { admitted: 0, failures: 0, reasons: deduce_reasons(result) }
      end

      failures = Forwarder.dispatch(claimed_rows, preinserted_inflight_ids: preinserted_ids)
      { admitted: claimed_rows.size - failures.size, failures: failures.size, reasons: [] }
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
