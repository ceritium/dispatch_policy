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
    def force!(policy_name:, partition_key:, limit:, retried: false)
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
            half_life_seconds: fairness_half_life(policy),
            # …and so does the throttle. Bypassing the gate's DECISION is
            # the point of this button; escaping its COST is not. Left
            # uncharged, a drain of N jobs hands the tenant N free plus a
            # whole untouched window, and the rate the policy declares
            # stops being true. Charged, the bucket goes into debt by
            # exactly what was forwarded and the next window repays it —
            # the same overdraft two racing tick loops produce.
            throttle_charge: throttle_charge_for(policy, policy_name, partition_key)
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
    rescue UndeliverableJob => e
      # Same as the Tick: the claim TX has rolled back, so the staged rows
      # are back; quarantine the offenders in their own write and try once
      # more, or the drain button can never get past a poisoned row.
      DispatchPolicy.config.logger&.error(
        "[dispatch_policy] undeliverable staged job in #{policy_name}/#{partition_key}, " \
        "quarantining: #{e.message}"
      )
      Repository.with_connection do
        Repository.quarantine_staged_jobs!(
          policy_name: policy_name, partition_key: partition_key,
          ids: e.staged_ids, reason: e.message
        )
      end
      raise if retried

      force!(policy_name: policy_name, partition_key: partition_key,
             limit: limit, retried: true)
    end

    # Same precedence the Tick uses: the policy's override, else the
    # global default. nil (fairness disabled) skips the decay clause,
    # which is then correct rather than an oversight.
    def fairness_half_life(policy)
      policy&.fairness_half_life_seconds || DispatchPolicy.config.fairness_half_life_seconds
    end

    # What the admission UPDATE needs to settle the bucket from the row's
    # own value. Only the capacity and the refill rate are needed — the
    # token count comes from the row — so a fixed `rate` and `per` are
    # enough and no context has to be reconstructed here.
    #
    # A proc rate or window cannot be resolved without one, and the ctx
    # gates read lives on the partition row, which this path never loads;
    # rather than charge against a guess, say so and leave the bucket
    # alone. Same for a policy this process's registry does not know —
    # see the warning at the top of force!.
    def throttle_charge_for(policy, policy_name, partition_key)
      return nil unless policy

      capacity    = policy.static_throttle_capacity
      refill_rate = policy.static_throttle_refill_rate
      if capacity.nil? || refill_rate.nil?
        if policy.gates.any? { |g| g.name == :throttle }
          DispatchPolicy.config.logger&.warn(
            "[dispatch_policy] force-admitting #{policy_name}/#{partition_key} without charging " \
            "its throttle: rate or per is a proc, and this path has no context to resolve it"
          )
        end
        return nil
      end

      { capacity: capacity, refill_rate: refill_rate, now: DispatchPolicy.config.now.to_f }
    end
  end
end
