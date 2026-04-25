# frozen_string_literal: true

module DispatchPolicy
  module Gates
    # Ordering gate that balances *compute time* across partitions, not
    # request count. Pair it with :throttle (the absolute ceiling) so a
    # solo tenant can run all the way up to its rate limit, while
    # concurrent tenants share the policy's effective throughput in
    # proportion to their use rather than their backlog size.
    #
    # Mechanics: at each tick we look at the consumed_ms per partition
    # over the last `window` (sourced from PartitionObservation, which
    # records perform duration generically), then walk the batch picking
    # the partition with the lowest accumulated "virtual time" that still
    # has pending work. After each pick, vtime advances by the partition's
    # recent average duration. Solo tenants always have the lowest vtime
    # so they're always picked → throughput is capped by their throttle,
    # not by us. Concurrent tenants converge on equal compute time.
    #
    # Doesn't admit or reject — it only reorders. Caps are still set by
    # whatever capping gates the policy declares.
    class FairTimeShare < Gate
      DEFAULT_WINDOW           = 60
      DEFAULT_DURATION_MS      = 100  # used when a partition has no observations yet

      def configure(window: DEFAULT_WINDOW, default_duration_ms: DEFAULT_DURATION_MS)
        @window              = window
        @default_duration_ms = default_duration_ms
        raise ArgumentError, "fair_time_share requires partition_by" if @partition_by.nil?
      end

      def tracks_inflight?
        false
      end

      attr_reader :window, :default_duration_ms

      def filter(batch, context)
        groups = batch.group_by { |staged| partition_key_for(context.for(staged)) }
        return batch if groups.size <= 1

        stats = PartitionObservation.consumed_ms_by_partition(
          policy_name:    policy.name,
          partition_keys: groups.keys,
          window:         @window
        )

        # Virtual-time scheduler: per-partition cursor seeded with
        # consumed_ms in the window. Each pick advances the cursor by the
        # partition's recent avg duration (or default if it has none yet).
        vtime    = Hash.new { |h, k| h[k] = (stats.dig(k, :consumed_ms) || 0).to_f }
        avg_dur  = Hash.new do |h, k|
          row   = stats[k]
          h[k]  = if row && row[:count].positive? && row[:consumed_ms].positive?
                    row[:consumed_ms].to_f / row[:count]
          else
                    @default_duration_ms.to_f
          end
        end

        ordered = []
        until groups.values.all?(&:empty?)
          pick = groups.each_key.select { |k| groups[k].any? }.min_by { |k| vtime[k] }
          ordered << groups[pick].shift
          vtime[pick] += avg_dur[pick]
        end
        ordered
      end
    end

    Gate.register(:fair_time_share, FairTimeShare)
  end
end
