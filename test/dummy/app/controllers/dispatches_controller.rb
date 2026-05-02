# frozen_string_literal: true

# One endpoint that every job-card form on the home page POSTs to.
# `kind` selects the job class; `attrs` is the per-card form fields
# (everything else is generic: count, wait_seconds, batch).
#
# Returns JSON when fetched (the layout's submit handler turns every
# `form.dispatch` into a fetch) so the page stays put and shows a
# flash message instead of a full reload.
class DispatchesController < ApplicationController
  KINDS = {
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

  def create
    kind  = params[:kind].to_s
    klass = KINDS[kind]
    return reply(alert: "unknown kind #{kind.inspect}", status: :unprocessable_entity) unless klass

    count    = params.fetch(:count, 1).to_i.clamp(1, 5_000)
    attrs    = params.fetch(:attrs, {}).permit!.to_h
    wait_sec = params[:wait_seconds].to_i
    batch    = params[:batch] == "1"

    if batch
      jobs = count.times.map { |i| klass.new(attrs.merge("seq" => i)) }
      ActiveJob.perform_all_later(jobs)
    else
      count.times do |i|
        kw = attrs.merge("seq" => i)
        if wait_sec.positive?
          klass.set(wait: wait_sec.seconds).perform_later(kw)
        else
          klass.perform_later(kw)
        end
      end
    end

    suffix = batch ? " (perform_all_later)" : (wait_sec.positive? ? " (wait #{wait_sec}s)" : "")
    reply(notice: "Enqueued #{count} #{klass}#{suffix}.")
  end

  private

  def reply(notice: nil, alert: nil, status: :ok)
    msg = notice || alert
    kind = notice ? :notice : :alert
    respond_to do |format|
      format.json { render json: { notice: notice, alert: alert }, status: status }
      format.html { redirect_to(root_path, kind => msg) }
    end
  end
end
