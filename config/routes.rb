# frozen_string_literal: true

DispatchPolicy::Engine.routes.draw do
  root to: "dashboard#index"

  resources :policies, only: %i[index show], param: :name, constraints: { name: %r{[^/]+} } do
    member do
      post :pause
      post :resume
    end
  end

  resources :partitions, only: %i[index show] do
    member do
      post :clear
      post :admit
    end
  end

  resources :staged_jobs, only: %i[show]
end
