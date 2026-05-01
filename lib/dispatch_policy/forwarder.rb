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
  module Forwarder
    module_function

    # @param rows [Array<Hash>] admitted staged_job rows (already deleted from staging)
    # @raise StandardError propagates any error from deserialize / adapter enqueue
    def dispatch(rows)
      return if rows.empty?

      rows.each do |row|
        job        = Serializer.deserialize(row["job_data"])
        wait_until = enqueue_wait_until(row)
        Bypass.with do
          if wait_until
            job.set(wait_until: wait_until).enqueue
          else
            job.enqueue
          end
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
