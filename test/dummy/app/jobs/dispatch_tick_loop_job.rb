# frozen_string_literal: true

class DispatchTickLoopJob < ApplicationJob
  queue_as { arguments[1].presence || :dispatch_loop }

  if ENV["DUMMY_ADAPTER"].to_s == "good_job" && defined?(GoodJob::ActiveJobExtensions::Concurrency)
    include GoodJob::ActiveJobExtensions::Concurrency
    # enqueue_limit + perform_limit, NOT total_limit: total_limit counts the
    # running job in its enqueue check, so the self-re-enqueue would be
    # aborted and the tick chain would die after the first run.
    good_job_control_concurrency_with(
      enqueue_limit: 1,
      perform_limit: 1,
      key: -> { "dispatch_tick_loop:#{arguments[0] || 'all'}:#{arguments[1] || 'all'}" }
    )
  elsif ENV["DUMMY_ADAPTER"].to_s == "solid_queue"
    if respond_to?(:limits_concurrency)
      limits_concurrency to: 1,
        key: -> { "dispatch_tick_loop:#{arguments[0] || 'all'}:#{arguments[1] || 'all'}" }
    end
  end

  def perform(policy_name = nil, shard = nil)
    deadline = Time.current + DispatchPolicy.config.tick_max_duration

    DispatchPolicy::TickLoop.run(
      policy_name: policy_name,
      shard:       shard,
      stop_when:   -> { adapter_shutting_down? || Time.current >= deadline }
    )

    successor = self.class.set(wait: 1.second).perform_later(policy_name, shard)

    if successor.respond_to?(:successfully_enqueued?) && !successor.successfully_enqueued?
      Rails.logger.error(
        "[dispatch_policy] DispatchTickLoopJob failed to re-enqueue itself " \
        "(policy=#{policy_name.inspect} shard=#{shard.inspect}) — the tick loop chain has stopped"
      )
    end
  end

  private

  def adapter_shutting_down?
    case ENV["DUMMY_ADAPTER"].to_s
    when "good_job"
      defined?(GoodJob) && GoodJob.respond_to?(:current_thread_shutting_down?) && GoodJob.current_thread_shutting_down?
    when "solid_queue"
      defined?(SolidQueue::Process) && SolidQueue::Process.current_process&.shutdown?
    else
      false
    end
  end
end
