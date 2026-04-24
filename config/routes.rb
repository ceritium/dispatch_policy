# frozen_string_literal: true

DispatchPolicy::Engine.routes.draw do
  root to: "policies#index"
  resources :policies, only: %i[index show], param: :policy_name, constraints: { policy_name: %r{[^/]+} }
end
