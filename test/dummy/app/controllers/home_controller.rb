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
    "fairness_demo"     => FairnessDemoJob,
    "adaptive_demo"     => AdaptiveDemoJob
  }.freeze

  def index
    @adapter = ENV["DUMMY_ADAPTER"] || "good_job"
    @jobs    = JOBS.keys
  end

  def enqueue
    job_class = JOBS[params[:job]]
    return redirect_to(root_path, alert: "Unknown job #{params[:job].inspect}") unless job_class

    attrs = clean_attrs
    delay = safe_int(params[:delay])
    if delay&.positive?
      job_class.set(wait: delay.seconds).perform_later(attrs)
    else
      job_class.perform_later(attrs)
    end
    redirect_to root_path, notice: "Enqueued one #{job_class}."
  end

  def enqueue_many
    job_class = JOBS[params[:job]]
    return redirect_to(root_path, alert: "Unknown job #{params[:job].inspect}") unless job_class

    count = (safe_int(params[:count]) || 10).clamp(1, 100_000)
    base  = clean_attrs

    jobs = count.times.map do |i|
      job_class.new(base.merge("seq" => i))
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
    hot_count  = (safe_int(params[:hot_count])  || 500).clamp(0, 100_000)
    cold_count = (safe_int(params[:cold_count]) || 10).clamp(0, 100_000)

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

  private

  # Strip blank form fields so the job context procs' Integer(attrs[...] ||
  # default) hit the default instead of Integer("") → ArgumentError → 500.
  def clean_attrs
    params.fetch(:attrs, {}).permit!.to_h.reject { |_, v| v.respond_to?(:empty?) ? v.empty? : v.nil? }
  end

  def safe_int(value)
    Integer(value, exception: false)
  end
end
