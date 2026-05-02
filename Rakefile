# frozen_string_literal: true

require "bundler/setup"
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
end

desc "Alias for bench:all"
task bench: "bench:all"
