# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
  # Mounted by the host app. Views, controllers, and AR models live under
  # `app/`; the layout inlines the engine CSS by reading
  # `app/assets/stylesheets/dispatch_policy/application.css` at render
  # time, and serves the vendored Turbo bundle through `AssetsController`
  # at a content-addressed URL — no asset pipeline integration required.
  class Engine < ::Rails::Engine
    isolate_namespace DispatchPolicy
  end
end
