# frozen_string_literal: true

class HomeController < ApplicationController
  JOBS = {
    "slow_external_api" => SlowExternalApiJob,
    "bulk_account"      => BulkAccountJob,
    "mixed"             => MixedJob,
    "perform_in"        => PerformInJob,
    "retry_flaky"       => RetryFlakyJob
  }.freeze

  def index
    @adapter = ENV["DUMMY_ADAPTER"] || "good_job"
    @jobs    = JOBS.keys
  end

  def enqueue
    job_class = JOBS.fetch(params[:job])
    attrs     = params.fetch(:attrs, {}).permit!.to_h
    if (delay = params[:delay].presence)
      job_class.set(wait: Integer(delay).seconds).perform_later(attrs)
    else
      job_class.perform_later(attrs)
    end
    redirect_to root_path, notice: "Enqueued one #{job_class}."
  end

  def enqueue_many
    job_class = JOBS.fetch(params[:job])
    count     = Integer(params.fetch(:count, 10))
    base      = params.fetch(:attrs, {}).permit!.to_h

    jobs = count.times.map do |i|
      attrs = base.merge("seq" => i)
      job_class.new(attrs)
    end

    if ActiveJob.respond_to?(:perform_all_later)
      ActiveJob.perform_all_later(jobs)
    else
      jobs.each(&:enqueue)
    end

    redirect_to root_path, notice: "Enqueued #{count} × #{job_class}."
  end
end
