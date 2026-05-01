# frozen_string_literal: true

module DispatchPolicy
  # Re-enqueues admitted jobs onto the real ActiveJob adapter under a
  # `Bypass.with` block, so the around_enqueue callback that staged
  # them in the first place lets the call through.
  module Forwarder
    Failure = Struct.new(:row, :error, keyword_init: true)

    module_function

    # @param rows [Array<Hash>] admitted staged_job rows (already deleted from staging)
    # @param preinserted_inflight_ids [Array<String>] active_job_ids the Tick
    #   pre-inserted into inflight_jobs; on per-job forward failure we delete
    #   the matching inflight row so concurrency budget stays accurate.
    # @return [Array<Failure>] failures (rows already reinserted into staging)
    def dispatch(rows, preinserted_inflight_ids: [])
      return [] if rows.empty?

      preinserted = preinserted_inflight_ids.to_set

      failures = []
      rows.each do |row|
        active_job_id = row.dig("job_data", "job_id")
        begin
          job = Serializer.deserialize(row["job_data"])
          wait_until = enqueue_wait_until(row)
          Bypass.with do
            if wait_until
              job.set(wait_until: wait_until).enqueue
            else
              job.enqueue
            end
          end
        rescue StandardError => e
          DispatchPolicy.config.logger&.error(
            "[dispatch_policy] forward failed for #{row['job_class']} (#{active_job_id || 'no-id'}): #{e.class}: #{e.message}"
          )
          failures << Failure.new(row: row, error: e)
          if active_job_id && preinserted.include?(active_job_id)
            Repository.delete_inflight!(active_job_id: active_job_id)
          end
        end
      end

      if failures.any?
        Repository.unclaim!(failures.map(&:row))
      end
      failures
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
