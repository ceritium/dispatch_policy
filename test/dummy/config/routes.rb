# frozen_string_literal: true

Rails.application.routes.draw do
  root "home#index"

  post "/enqueue/:job",      to: "home#enqueue",      as: :enqueue
  post "/enqueue_many/:job", to: "home#enqueue_many", as: :enqueue_many

  mount DispatchPolicy::Engine, at: "/dispatch_policy"

  case (ENV["DUMMY_ADAPTER"] || "good_job")
  when "good_job"
    mount GoodJob::Engine, at: "/good_job" if defined?(GoodJob::Engine)
  end
end
