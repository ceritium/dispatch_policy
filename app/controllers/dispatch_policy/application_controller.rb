# frozen_string_literal: true

module DispatchPolicy
  class ApplicationController < ActionController::Base
    protect_from_forgery with: :exception

    layout "dispatch_policy/application"
  end
end
