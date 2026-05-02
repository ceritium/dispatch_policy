# frozen_string_literal: true

# Stress-test trigger: kicks off StormJob in the background which then
# floods one of the partitioned policies with many distinct tenants.
# The point is to exercise:
#
#   - the admin UI under hundreds of partitions
#   - the in-tick fairness reorder under a skewed load
#   - the adaptive_concurrency cap reacting to bursts and idle
#
# StormJob runs for `duration_seconds` so the operator can watch the
# admit pattern evolve in /dispatch_policy without re-clicking.
class StormsController < ApplicationController
  def create
    num_accounts     = params[:num_accounts].to_i.clamp(1, 2_000)
    duration_seconds = params[:duration_seconds].to_i.clamp(1, 600)
    batch_size       = params[:batch_size].to_i.clamp(1, 200)
    target           = params[:target].presence || "adaptive_demo"

    StormJob.perform_later(
      num_accounts:     num_accounts,
      duration_seconds: duration_seconds,
      batch_size:       batch_size,
      target:           target
    )

    msg = "Storm started: target=#{target}, #{num_accounts} accounts, #{duration_seconds}s, batch_size=#{batch_size}."
    respond_to do |format|
      format.json { render json: { notice: msg } }
      format.html { redirect_to root_path, notice: msg }
    end
  end
end
