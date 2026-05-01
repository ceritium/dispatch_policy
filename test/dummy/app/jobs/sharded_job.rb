# frozen_string_literal: true

# Demonstrates `shard_by`. Each job is routed to a queue derived from the
# account_id hash; the policy's `shard_by` simply mirrors that queue, so
# shard == queue. The operator launches one DispatchTickLoopJob per shard
# (each on its own queue) and the same worker pool that runs the tick loop
# also picks up admitted jobs:
#
#   4.times do |i|
#     DispatchTickLoopJob.perform_later("sharded", "shard-#{i}")
#   end
class ShardedJob < ApplicationJob
  # 4-way fanout on the account_id. The queue name is the shard.
  queue_as do
    attrs = arguments.first || {}
    "shard-#{(attrs["account_id"] || attrs[:account_id] || "default").to_s.hash.abs % 4}"
  end

  dispatch_policy_inflight_tracking

  dispatch_policy :sharded do
    context ->(args) {
      attrs = args.first || {}
      {
        account_id: attrs["account_id"] || attrs[:account_id] || "default",
        max:        Integer(attrs["max"] || attrs[:max] || 5)
      }
    }

    # ctx[:queue_name] is already set by the framework to the result of
    # queue_as above, so shard == queue. This is the canonical "tick and
    # workers share one queue per shard" pattern.
    shard_by ->(c) { c[:queue_name] }

    gate :concurrency,
         max:          ->(c) { c[:max] },
         partition_by: ->(c) { "acct:#{c[:account_id]}" }
  end

  def perform(attrs = {})
    sleep(Float(ENV.fetch("SHARDED_SLEEP", "0.2")))
    Rails.logger.info("[ShardedJob] acct=#{attrs['account_id']} ran at #{Time.current.iso8601}")
  end
end
