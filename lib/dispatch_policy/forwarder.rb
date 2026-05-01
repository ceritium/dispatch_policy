# frozen_string_literal: true

module DispatchPolicy
  # Re-enqueues admitted jobs onto the real ActiveJob adapter under a
  # `Bypass.with` block, so the around_enqueue callback that staged them
  # in the first place lets the call through.
  #
  # Called from inside Tick's admission transaction. With a PG-backed
  # adapter (good_job / solid_queue) the adapter's INSERT shares the
  # transaction, so any exception here aborts the whole admission
  # atomically (staged_jobs return, inflight rows disappear, partition
  # counters revert, adapter rows revert). There is intentionally no
  # rescue here: failures must propagate to roll back the surrounding TX.
  #
  # Bulk path: rows without scheduled_at go through ActiveJob.perform_all_later,
  # which collapses to a single multi-row INSERT on adapters that implement
  # enqueue_all natively (good_job, solid_queue). Rows with scheduled_at
  # keep the per-row path because perform_all_later doesn't accept a
  # wait_until per job.
  module Forwarder
    module_function

    # @param rows [Array<Hash>] admitted staged_job rows (already deleted from staging)
    # @raise StandardError propagates any error from deserialize / adapter enqueue
    # @raise EnqueueFailed if the adapter's enqueue_all returned without
    #   raising but flagged any job as not-successfully-enqueued (the
    #   atomic contract requires caller-visible failure so the surrounding
    #   TX rolls back).
    def dispatch(rows)
      return if rows.empty?

      scheduled, immediate = rows.partition { |row| row["scheduled_at"] }

      if immediate.any?
        jobs = immediate.map { |row| Serializer.deserialize(row["job_data"]) }
        Bypass.with { ::ActiveJob.perform_all_later(jobs) }
        not_enqueued = jobs.reject { |j| j.respond_to?(:successfully_enqueued?) ? j.successfully_enqueued? : true }
        if not_enqueued.any?
          ids = not_enqueued.map(&:job_id).join(", ")
          raise EnqueueFailed,
                "perform_all_later soft-failed #{not_enqueued.size}/#{jobs.size} jobs (#{ids})"
        end
      end

      scheduled.each do |row|
        job        = Serializer.deserialize(row["job_data"])
        wait_until = enqueue_wait_until(row)
        Bypass.with { job.set(wait_until: wait_until).enqueue }
        if job.respond_to?(:successfully_enqueued?) && !job.successfully_enqueued?
          raise EnqueueFailed, "scheduled enqueue soft-failed for #{job.job_id}"
        end
      end
    end

    def enqueue_wait_until(row)
      ts = row["scheduled_at"]
      return nil unless ts
      ts.is_a?(Time) ? ts : Time.parse(ts.to_s)
    rescue ArgumentError
      nil
    end
  end
end
