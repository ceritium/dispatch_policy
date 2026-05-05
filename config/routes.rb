# frozen_string_literal: true

DispatchPolicy::Engine.routes.draw do
  root to: "dashboard#index"

  resources :policies, only: %i[index show], param: :name, constraints: { name: %r{[^/]+} } do
    member do
      post :pause
      post :resume
      post :drain
    end
  end

  resources :partitions, only: %i[index show] do
    member do
      post :drain
      post :admit
    end
  end

  resources :staged_jobs, only: %i[show]

  get "assets/turbo-:digest.js",  to: "assets#turbo", as: :turbo_asset
  get "assets/logo-:digest.svg",  to: "assets#logo",  as: :logo_asset
end
