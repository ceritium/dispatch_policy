# frozen_string_literal: true

module DispatchPolicy
  class DispatchContext
    def initialize(policy:, batch:)
      @policy     = policy
      @cache      = {}
      @partitions = Hash.new { |h, k| h[k] = {} }
      batch.each { |staged| resolve_for(staged) }
    end

    def for(staged)
      @cache[staged.id]
    end

    def record_partitions(pairs, gate:)
      pairs.each { |staged, partition_key| @partitions[staged.id][gate.to_sym] = partition_key.to_s }
    end

    def partitions_for(staged)
      @partitions[staged.id]
    end

    def primary_partition_for(staged)
      @partitions[staged.id].values.first
    end

    private

    def resolve_for(staged)
      cached = staged.context
      if cached.is_a?(Hash) && cached.present?
        @cache[staged.id] = cached.symbolize_keys
        return
      end

      # Fallback: recompute from the serialized args. Hit on rows staged
      # before the context column existed, or when context_builder
      # legitimately returned an empty hash.
      raw  = (staged.arguments || {})["arguments"] || []
      args = begin
        ActiveJob::Arguments.deserialize(raw)
      rescue StandardError => e
        Rails.logger&.warn(
          "[DispatchPolicy] could not deserialize args for staged=#{staged.id} " \
          "(policy=#{staged.policy_name}): #{e.class}: #{e.message}"
        )
        raw
      end
      @cache[staged.id] = @policy.context_builder.call(args)
    end
  end
end
