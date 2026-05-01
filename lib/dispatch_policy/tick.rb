# frozen_string_literal: true

module DispatchPolicy
  # One pass of admission for a single policy.
  #
  # 1. Claim a batch of partitions (FOR UPDATE SKIP LOCKED).
  # 2. For each partition, evaluate the gate pipeline against the partition's
  #    fresh `context` and current `gate_state`.
  # 3. Atomically DELETE … RETURNING staged rows up to the admitted budget,
  #    update the partition's counters, persist gate_state patch and
  #    next_eligible_at, and pre-insert inflight rows for concurrency-gated
  #    jobs (idempotent vs the around_perform tracker).
  # 4. After commit, re-enqueue the admitted jobs to the real adapter.
  #    On forward failure, compensate by reinserting into staged_jobs.
  class Tick
    Result = Struct.new(:partitions_seen, :jobs_admitted, keyword_init: true)

    def self.run(policy_name:)
      new(policy_name).call
    end

    def initialize(policy_name)
      @policy_name = policy_name
      @policy      = DispatchPolicy.registry.fetch(policy_name) || raise(InvalidPolicy, "unknown policy #{policy_name.inspect}")
      @config      = DispatchPolicy.config
    end

    def call
      partitions_seen = 0
      jobs_admitted   = 0

      partitions = Repository.claim_partitions(
        policy_name: @policy_name,
        limit:       @config.partition_batch_size
      )

      partitions.each do |partition|
        partitions_seen += 1
        admitted = admit_partition(partition)
        jobs_admitted += admitted
      end

      Result.new(partitions_seen: partitions_seen, jobs_admitted: jobs_admitted)
    end

    private

    def admit_partition(partition)
      ctx     = Context.wrap(partition["context"])
      pipe    = Pipeline.new(@policy)
      max_budget = @policy.admission_batch_size || @config.admission_batch_size
      result  = pipe.call(ctx, partition, max_budget)

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
            concurrency_gate = @policy.gates.find { |g| g.name == :concurrency }
            if concurrency_gate
              inflight_rows = rows.filter_map do |row|
                ajid = row.dig("job_data", "job_id")
                next unless ajid

                preinserted_ids << ajid
                {
                  policy_name:   @policy_name,
                  partition_key: concurrency_gate.inflight_partition_key(@policy_name, Context.wrap(row["context"])),
                  active_job_id: ajid
                }
              end
              Repository.insert_inflight!(inflight_rows) if inflight_rows.any?
            end
          end

          rows
        end

      return 0 if claimed_rows.empty?

      failures = Forwarder.dispatch(claimed_rows, preinserted_inflight_ids: preinserted_ids)
      claimed_rows.size - failures.size
    end
  end
end
