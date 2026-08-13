# frozen_string_literal: true

# Runs every bench_*.rb script in this directory in turn and prints a
# combined Markdown report on stdout.
#
#   bundle exec ruby test/benchmark/run_all.rb
#   RUNS=10 BIG_SCALES=1 bundle exec ruby test/benchmark/run_all.rb
#
# Each script handles its own DB setup (truncate + seed) so the order
# is irrelevant. They all share the schema bootstrapped by the first.

require_relative "bench_helper"

Bench.connect!
Bench.recreate_schema!

# Skip bench_helper.rb (required by every bench_*.rb), the real-adapter
# bench (boots the dummy Rails app, separate via `rake bench:real`),
# and bench_limits.rb (slow, runs to breaking point — separate via
# `rake bench:limits`).
scripts = Dir[File.join(__dir__, "bench_*.rb")]
            .reject { |p| %w[bench_helper.rb bench_real_adapter.rb bench_limits.rb].include?(File.basename(p)) }
            .sort

# FILTER is the documented knob (see the Rakefile). ARGV is only ours
# when this file is run directly — under `rake bench:all` ARGV holds the
# rake task name, and "bench_claim".include?("bench:all") is false for
# every script, so reading ARGV there silently ran NOTHING and reported
# success. That is the exact "a benchmark stopped running" failure the
# suite exists to catch, so it must not be possible to reintroduce by
# invocation style.
filter = ENV["FILTER"] || (ARGV.first unless defined?(Rake))

scripts.each do |path|
  name = File.basename(path, ".rb")
  next if filter && !name.include?(filter)

  puts "\n"
  puts "=" * 78
  puts "# #{name}"
  puts "=" * 78
  load path
end
