# frozen_string_literal: true

class HomeController < ApplicationController
  JOBS = {
    "slow_external_api" => SlowExternalApiJob,
    "bulk_account"      => BulkAccountJob,
    "mixed"             => MixedJob,
    "perform_in"        => PerformInJob,
    "retry_flaky"       => RetryFlakyJob,
    "high_concurrency"  => HighConcurrencyJob,
    "high_throttle"     => HighThrottleJob,
    "sharded"           => ShardedJob,
    "fairness_demo"     => FairnessDemoJob
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

  # Builds an intentionally uneven backlog so the in-tick fairness
  # ordering is visible. One tenant gets `hot_count` jobs; four others
  # get `cold_count` each. With FairnessDemoJob's tick_admission_budget,
  # cold tenants drain in 1-2 ticks while the hot one progresses at the
  # capped rate.
  def fairness_demo_flood
    hot_count  = Integer(params.fetch(:hot_count, 500))
    cold_count = Integer(params.fetch(:cold_count, 10))

    plan = [["hot", hot_count]] +
           %w[cold-1 cold-2 cold-3 cold-4].map { |t| [t, cold_count] }

    plan.each do |tenant, n|
      jobs = n.times.map { |i| FairnessDemoJob.new("tenant" => tenant, "seq" => i) }
      ActiveJob.perform_all_later(jobs)
    end

    total = hot_count + cold_count * 4
    redirect_to root_path,
                notice: "Enqueued #{total} FairnessDemoJob across 5 tenants " \
                        "(1 × #{hot_count} hot + 4 × #{cold_count} cold). " \
                        "Watch /dispatch_policy/policies/fairness_demo."
  end
end
