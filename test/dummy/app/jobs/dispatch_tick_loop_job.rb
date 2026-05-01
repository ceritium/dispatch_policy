# frozen_string_literal: true

class DispatchTickLoopJob < ApplicationJob
  self.queue_adapter = ActiveJob::Base.queue_adapter
  queue_as :dispatch_loop

  if ENV["DUMMY_ADAPTER"].to_s == "good_job" && defined?(GoodJob::ActiveJobExtensions::Concurrency)
    include GoodJob::ActiveJobExtensions::Concurrency
    good_job_control_concurrency_with(
      total_limit: 1,
      key: -> { "dispatch_tick_loop:#{arguments.first || 'all'}" }
    )
  elsif ENV["DUMMY_ADAPTER"].to_s == "solid_queue"
    if respond_to?(:limits_concurrency)
      limits_concurrency to: 1, key: -> { "dispatch_tick_loop:#{arguments.first || 'all'}" }
    end
  end

  def perform(policy_name = nil)
    deadline = Time.current + DispatchPolicy.config.tick_max_duration

    DispatchPolicy::TickLoop.run(
      policy_name: policy_name,
      stop_when:   -> { adapter_shutting_down? || Time.current >= deadline }
    )

    self.class.set(wait: 1.second).perform_later(policy_name)
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
