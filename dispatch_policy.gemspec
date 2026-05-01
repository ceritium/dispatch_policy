# frozen_string_literal: true

require_relative "lib/dispatch_policy/version"

Gem::Specification.new do |spec|
  spec.name        = "dispatch_policy"
  spec.version     = DispatchPolicy::VERSION
  spec.authors     = ["José Galisteo"]
  spec.email       = ["ceritium@gmail.com"]
  spec.summary     = "Per-partition admission control for ActiveJob (Postgres)."
  spec.description = "Stages perform_later into a dedicated table, runs a tick loop that admits jobs through declared gates (throttle, concurrency), then forwards survivors to the real ActiveJob adapter. Embedded as a periodic job. Compatible with good_job and solid_queue."
  spec.homepage    = "https://github.com/ceritium/dispatch_policy"
  spec.license     = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  spec.files = Dir[
    "lib/**/*.rb",
    "lib/generators/**/*",
    "app/**/*",
    "config/**/*",
    "db/**/*",
    "MIT-LICENSE",
    "README.md"
  ]
  spec.require_paths = ["lib"]

  spec.add_dependency "rails", ">= 7.1"
  spec.add_dependency "pg", ">= 1.4"

  spec.add_development_dependency "minitest", "~> 5.20"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "good_job", ">= 4.0"
  spec.add_development_dependency "solid_queue", ">= 1.0"
  spec.add_development_dependency "turbo-rails", ">= 1.5"
  spec.add_development_dependency "puma", ">= 6.0"
  spec.add_development_dependency "foreman", ">= 0.87"
  spec.add_development_dependency "debug"
end
