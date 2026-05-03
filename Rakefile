# frozen_string_literal: true

require "bundler/setup"
require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.libs << "test"
  t.libs << "lib"
  t.test_files = FileList["test/unit/**/*_test.rb", "test/integration/**/*_test.rb"]
  t.warning = false
end

task default: :test

namespace :bench do
  desc "Run all benchmarks (talks to Postgres; slow). " \
       "Pass FILTER=tick to limit, RUNS=10 to increase samples, " \
       "BIG_SCALES=1 to include 100k-partition scenarios."
  task :all do
    require_relative "test/benchmark/run_all"
  end

  %w[tick stage claim forwarder].each do |name|
    desc "Run bench_#{name}.rb only"
    task name.to_sym do
      load File.expand_path("test/benchmark/bench_#{name}.rb", __dir__)
    end
  end

  desc "Real-adapter end-to-end bench. Defaults to good_job on the dummy DB. " \
       "Override via BENCH_ADAPTER=solid_queue and/or BENCH_DB_NAME=…. " \
       "Requires the adapter's tables to exist (run `bin/dummy setup good_job` first)."
  task :real do
    load File.expand_path("test/benchmark/bench_real_adapter.rb", __dir__)
  end

  desc "Stretch every path to its breaking point. Set BENCH_DB_NAME=dispatch_policy_dummy " \
       "for end-to-end limits including the good_job INSERT; default DB stays gem-only."
  task :limits do
    load File.expand_path("test/benchmark/bench_limits.rb", __dir__)
  end
end

desc "Alias for bench:all"
task bench: "bench:all"
