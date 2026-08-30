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
      immediate_jobs = immediate.map { |row| deserialize!(row) }
      scheduled_jobs = scheduled.map { |row| [deserialize!(row), enqueue_wait_until(row)] }

      enqueuing_inline(immediate_jobs + scheduled_jobs.map(&:first)) do
        if immediate_jobs.any?
          Bypass.with { ::ActiveJob.perform_all_later(immediate_jobs) }
          not_enqueued = immediate_jobs.reject { |j| j.respond_to?(:successfully_enqueued?) ? j.successfully_enqueued? : true }
          if not_enqueued.any?
            ids = not_enqueued.map(&:job_id).join(", ")
            raise EnqueueFailed,
                  "perform_all_later soft-failed #{not_enqueued.size}/#{immediate_jobs.size} jobs (#{ids})"
          end
        end

        scheduled_jobs.each do |job, wait_until|
          Bypass.with { job.set(wait_until: wait_until).enqueue }
          if job.respond_to?(:successfully_enqueued?) && !job.successfully_enqueued?
            raise EnqueueFailed, "scheduled enqueue soft-failed for #{job.job_id}"
          end
        end
      end
    end

    # ActiveJob 7.2+ lets a job class defer its own enqueue past the
    # surrounding transaction with `self.enqueue_after_transaction_commit =
    # true` — the setting Rails recommends for apps that enqueue inside AR
    # transactions. That is fatal here: the forward runs INSIDE the
    # admission TX, so the deferral registers the real enqueue on OUR
    # transaction and it lands after COMMIT, outside the Bypass window.
    # The scheduled path then re-stages the job it just admitted, forever
    # — one leaked inflight row per tick, so a concurrency gate wedges at
    # max — and the immediate path sees `successfully_enqueued? == false`,
    # raises, and rolls the admission back on every tick, forever. The job
    # never reaches the adapter either way, and nothing says so.
    #
    # `ActiveRecord.after_all_transactions_commit` runs its block inline
    # when `all_open_transactions` is empty, and that list skips
    # transactions that are not joinable. Opening a non-joinable savepoint
    # around the enqueue therefore makes the deferral a no-op: the work
    # happens here, inside the admission TX and inside Bypass, which is
    # the whole contract. Only done when a job in the batch actually
    # defers — a non-joinable savepoint runs its commit callbacks on
    # RELEASE rather than at the real COMMIT, and there is no reason to
    # impose that on the deployments that do not need it.
    def enqueuing_inline(jobs, &block)
      return yield unless jobs.any? { |job| defers_its_own_enqueue?(job) }

      # `transaction` swallows ActiveRecord::Rollback by design, so a
      # savepoint here would absorb one raised anywhere in the forward:
      # dispatch returns normally, the Tick counts the admission, and the
      # TX commits with the staged rows deleted, the inflight rows
      # inserted, and NOTHING in the adapter. Without the savepoint that
      # same exception reaches admit_partition's own transaction and
      # aborts the admission. Re-raise so the two paths agree.
      completed = false
      ActiveRecord::Base.transaction(requires_new: true, joinable: false) do
        block.call
        completed = true
      end
      return if completed

      raise EnqueueFailed,
            "the forward was rolled back inside its own savepoint; the admission must not commit"
    end

    def defers_its_own_enqueue?(job)
      job.class.respond_to?(:enqueue_after_transaction_commit) &&
        job.class.enqueue_after_transaction_commit
    end

    # A row whose job_class no longer resolves can never be delivered: the
    # deploy that renamed or dropped the constant is not coming back on
    # the next tick. Raising a plain NameError here rolls the batch back
    # (correct — that rollback is the at-least-once guarantee) and leaves
    # the row at the head of the claim, where it poisons every subsequent
    # admission of that partition forever, healthy neighbours included.
    # UndeliverableJob names the offending ids so the caller can
    # quarantine exactly those and admit the rest.
    def deserialize!(row)
      Serializer.deserialize(row["job_data"])
    rescue NameError, InvalidPolicy => e
      raise UndeliverableJob.new(
        "staged row #{row['id']} (#{row['job_class']}): #{e.class}: #{e.message}",
        staged_ids: [row["id"]]
      )
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
