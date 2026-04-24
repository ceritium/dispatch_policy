# frozen_string_literal: true

Rails.application.routes.draw do
  mount DispatchPolicy::Engine => "/dispatch_policy"
end
