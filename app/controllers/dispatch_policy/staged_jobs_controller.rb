# frozen_string_literal: true

module DispatchPolicy
  class StagedJobsController < ApplicationController
    def show
      @job = StagedJob.find(params[:id])
    end
  end
end
