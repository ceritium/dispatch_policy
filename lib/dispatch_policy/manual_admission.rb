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

      forwarded = 0
      Repository.with_connection do
        ActiveRecord::Base.transaction(requires_new: true) do
          rows = Repository.claim_staged_jobs!(
            policy_name:      policy_name,
            partition_key:    partition_key,
            limit:            limit,
            gate_state_patch: {},
            retry_after:      nil
          )
          next if rows.empty?

          rows.each { |row| row["job_data"]["job_id"] = SecureRandom.uuid }

          # Pre-insert an inflight row per admitted job, exactly like
          # Tick#admit_partition does. Without it the concurrency gate's
          # COUNT(*) misses these jobs until each one starts performing and
          # InflightTracker.track inserts its own row — an over-admission
          # window proportional to how many jobs were force-admitted. The
          # key is the canonical partition value, which for a policy-level
          # partition_by is exactly the staged partition_key (see
          # Concurrency#inflight_partition_key). Runs inside the same TX, so
          # a rolled-back claim takes the inflight rows with it.
          inflight_rows = rows.filter_map do |row|
            ajid = row.dig("job_data", "job_id")
            next unless ajid

            { policy_name: policy_name, partition_key: partition_key, active_job_id: ajid }
          end
          Repository.insert_inflight!(inflight_rows) if inflight_rows.any?

          Forwarder.dispatch(rows)
          forwarded = rows.size
        end
      end
      forwarded
    end
  end
end
