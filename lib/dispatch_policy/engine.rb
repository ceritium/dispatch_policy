# frozen_string_literal: true

require "rails/engine"

module DispatchPolicy
  # Mounted by the host app. Views, controllers, and AR models live under
  # `app/`; the layout inlines the engine CSS by reading
  # `app/assets/stylesheets/dispatch_policy/application.css` at render time,
  # so no asset pipeline integration is required.
  class Engine < ::Rails::Engine
    isolate_namespace DispatchPolicy
  end
end
