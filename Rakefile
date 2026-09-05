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

namespace :mutations do
  desc "Break each load-bearing line in turn and check a test notices. " \
       "Slow (one full suite per mutation; talks to Postgres). " \
       "Pass FILTER=19, FILTER=forwarder or FILTER='rescue' to limit."
  task :all do
    require_relative "test/mutations/run"
    DispatchPolicy::Mutations::Runner.run
  end

  # A NO TARGET is reported only after the runner has copied the tree and
  # run a control suite — fifteen minutes to learn that a `find` string
  # moved. Every edit to a mutated line silently stales its entry, and a
  # stale entry proves nothing while looking exactly like a passing one.
  desc "Check every catalogue find-string still exists (seconds, no suite)"
  task :check do
    require_relative "test/mutations/catalogue"
    stale = DispatchPolicy::Mutations::ALL.reject { |m| File.read(m[:file]).include?(m[:find]) }
    if stale.empty?
      puts "all #{DispatchPolicy::Mutations::ALL.size} find-strings present"
    else
      stale.each { |m| puts "  STALE  #{m[:id]}  #{m[:label]}  (#{m[:file]})" }
      abort "#{stale.size} mutation(s) no longer match their source — they prove nothing until fixed."
    end
  end

  desc "List the catalogue without running anything"
  task :list do
    require_relative "test/mutations/catalogue"
    DispatchPolicy::Mutations::ALL.each do |m|
      expected = DispatchPolicy::Mutations::EXPECTED_SURVIVORS.key?(m[:id]) ? "  [expected survivor]" : ""
      puts format("  %-4s %-58s %s%s", m[:id], m[:label], m[:file], expected)
    end
  end
end

desc "Alias for mutations:all"
task mutations: "mutations:all"
