# frozen_string_literal: true

module DispatchPolicy
  module Gates
    class FairInterleave < Gate
      def configure(**_); end

      def filter(batch, context)
        groups = batch.group_by do |staged|
          if @partition_by
            partition_key_for(context.for(staged))
          else
            context.primary_partition_for(staged) || staged.id
          end
        end
        interleaved = []
        loop do
          taken = false
          groups.each_value do |g|
            next if g.empty?
            interleaved << g.shift
            taken = true
          end
          break unless taken
        end
        interleaved
      end
    end

    Gate.register(:fair_interleave, FairInterleave)
  end
end
