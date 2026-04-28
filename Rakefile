# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  # Exclude the benchmark directory from the default test run — those
  # are scripts, not pass/fail tests, and they take long enough to
  # warrant manual invocation via `rake test:benchmark`.
  t.test_files = FileList["test/**/*_test.rb"].exclude(%r{^test/benchmark/})
  t.warning = false
end

namespace :test do
  desc "Run dispatch_policy performance benchmarks (manual, prints a markdown report)"
  task :benchmark do
    $LOAD_PATH.unshift("test", "lib")
    load File.expand_path("test/benchmark/run_benchmarks.rb", __dir__)
  end
end

task default: :test
