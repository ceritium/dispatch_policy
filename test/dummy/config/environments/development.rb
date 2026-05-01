# frozen_string_literal: true

Rails.application.configure do
  config.cache_classes        = false
  config.eager_load           = false
  config.consider_all_requests_local       = true
  config.action_controller.perform_caching = false
  config.action_dispatch.show_exceptions   = :all
  config.active_support.deprecation        = :stderr

  config.cache_store         = :null_store
  config.action_mailer.perform_caching     = false
  config.active_record.migration_error     = :page_load
  config.active_record.verbose_query_logs  = true
  config.active_record.dump_schema_after_migration = false

  config.hosts.clear if config.respond_to?(:hosts)

  config.session_store :cookie_store, key: "_dispatch_policy_dummy_session"
end
