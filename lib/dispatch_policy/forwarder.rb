# frozen_string_literal: true

module DispatchPolicy
  # Re-enqueues admitted jobs onto the real ActiveJob adapter under a
  # `Bypass.with` block, so the around_enqueue callback that staged
  # them in the first place lets the call through.
  module Forwarder
    Failure = Struct.new(:row, :error, keyword_init: true)

    module_function

    # @return [Array<Hash>] rows that failed to forward (already unclaimed)
    def dispatch(rows)
      return [] if rows.empty?

      failures = []
      rows.each do |row|
        begin
          job = Serializer.deserialize(row["job_data"])
          enqueue_options = build_enqueue_options(row)
          Bypass.with do
            if enqueue_options[:wait_until]
              job.set(wait_until: enqueue_options[:wait_until]).enqueue
            else
              job.enqueue
            end
          end
        rescue StandardError => e
          DispatchPolicy.config.logger&.error("[dispatch_policy] forward failed: #{e.class}: #{e.message}")
          failures << Failure.new(row: row, error: e)
        end
      end

      if failures.any?
        Repository.unclaim!(failures.map(&:row))
      end
      failures
    end

    def build_enqueue_options(row)
      out = {}
      if row["scheduled_at"]
        ts = row["scheduled_at"]
        out[:wait_until] = ts.is_a?(Time) ? ts : Time.parse(ts.to_s)
      end
      out
    end
  end
end
