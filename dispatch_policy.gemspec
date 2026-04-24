# frozen_string_literal: true

require_relative "lib/dispatch_policy/version"

Gem::Specification.new do |spec|
  spec.name        = "dispatch_policy"
  spec.version     = DispatchPolicy::VERSION
  spec.authors     = [ "José Galisteo" ]
  spec.email       = [ "ceritium@gmail.com" ]
  spec.summary     = "Per-partition admission control (throttle, concurrency, dedupe, fairness) for ActiveJob."
  spec.description = "DispatchPolicy stages ActiveJob enqueues into a policy table and admits them through declared gates. Supports per-partition throttle/concurrency, dedupe, round-robin fairness, and ships a minimal Rails engine to inspect pending/admitted state."
  spec.license     = "MIT"

  spec.required_ruby_version = ">= 3.1"

  spec.files = Dir[
    "lib/**/*",
    "app/**/*",
    "config/**/*",
    "db/**/*",
    "MIT-LICENSE",
    "README.md"
  ]

  spec.add_dependency "activejob",    ">= 7.1"
  spec.add_dependency "activerecord", ">= 7.1"
  spec.add_dependency "railties",     ">= 7.1"

  spec.add_development_dependency "pg"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "sprockets-rails"
end
