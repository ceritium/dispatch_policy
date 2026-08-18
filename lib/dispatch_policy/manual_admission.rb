# frozen_string_literal: true

require "securerandom"

module DispatchPolicy
  # Force-admit staged jobs for a partition, bypassing every gate. Backs
  # the engine UI's "admit" and "drain" buttons.
  #
  # Mirrors the atomicity guarantee of Tick#admit_partition: the DELETE
  # from staged_jobs (via Repository.claim_staged_jobs!) and the adapter
  # enqueue (Forwarder.dispatch) run in a SINGLE transaction, so any
  # failure — deserialize, adapter, network — rolls the claim back and the
  # staged rows survive. Without this the UI buttons would DELETE staged
  # rows and then lose them whenever the forward raised, breaking the
  # gem's at-least-once contract on a path the Tick already protects.
  #
  # active_job_id is regenerated per row for the same reason Tick does it
  # (see Tick#admit_partition): adapters that key their jobs table on
  # active_job_id (good_job, solid_queue) raise RecordNotUnique against a
  # residual row from a previous admission, which would abort the TX.
  module ManualAdmission
    module_function

    # Force-admit up to `limit` staged jobs for the partition, bypassing
    # all gates, atomically. Returns the number of jobs forwarded.
    def force!(policy_name:, partition_key:, limit:)
      return 0 unless limit.positive?

      # Unlike Tick — which raises on an unknown policy — this runs in the
      # web process, whose registry is populated as a side effect of job
      # classes being loaded. Under lazy loading, or in a deployment that
      # serves the dashboard without ever referencing a job class, the
      # policy can legitimately be missing here while the workers know it
      # perfectly well. InflightTracker.pre_insert_admitted! therefore
      # errs toward inserting when the policy is unknown; warn so the
      # operator learns why the UI is guessing.
      policy = DispatchPolicy.registry.fetch(policy_name)
      if policy.nil?
        DispatchPolicy.config.logger&.warn(
          "[dispatch_policy] force-admitting #{policy_name}/#{partition_key} but this process's " \
          "registry doesn't know that policy; pre-inserting inflight rows conservatively"
        )
      end

      forwarded = 0
      Repository.with_connection do
        ActiveRecord::Base.transaction(requires_new: true) do
          rows = Repository.claim_staged_jobs!(
            policy_name:      policy_name,
            partition_key:    partition_key,
            limit:            limit,
            gate_state_patch: {},
            retry_after:      nil,
            # A forced admission bypasses the gates, so it has learned
            # nothing about capacity and must not clear a backoff one of
            # them asked for — that only makes the next tick re-claim the
            # partition, re-evaluate it and back it off again.
            preserve_next_eligible: true,
            # …but it IS an admission, so fairness has to see it. Without
            # the half-life the decay clause is skipped entirely and a
            # partition drained from the UI still looks under-admitted to
            # the next tick's reorder, which then favours it again.
            half_life_seconds: fairness_half_life(policy)
          )
          next if rows.empty?

          rows.each { |row| row["job_data"]["job_id"] = SecureRandom.uuid }

          # Pre-insert an inflight row per admitted job, through the same
          # helper the Tick uses. Without it the concurrency gate's COUNT(*)
          # misses these jobs until each one starts performing — an
          # over-admission window proportional to how many jobs were
          # force-admitted. Runs inside the same TX, so a rolled-back claim
          # takes the inflight rows with it.
          InflightTracker.pre_insert_admitted!(
            policy_name:   policy_name,
            policy:        policy,
            partition_key: partition_key,
            rows:          rows
          )

          Forwarder.dispatch(rows)
          forwarded = rows.size
        end
      end
      forwarded
    end

    # Same precedence the Tick uses: the policy's override, else the
    # global default. nil (fairness disabled) skips the decay clause,
    # which is then correct rather than an oversight.
    def fairness_half_life(policy)
      policy&.fairness_half_life_seconds || DispatchPolicy.config.fairness_half_life_seconds
    end
  end
end
