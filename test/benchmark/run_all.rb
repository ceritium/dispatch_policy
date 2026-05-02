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

# Skip bench_helper.rb itself (it's required by every bench_*.rb).
scripts = Dir[File.join(__dir__, "bench_*.rb")]
            .reject { |p| File.basename(p) == "bench_helper.rb" }
            .sort
filter  = ARGV.first

scripts.each do |path|
  name = File.basename(path, ".rb")
  next if filter && !name.include?(filter)

  puts "\n"
  puts "=" * 78
  puts "# #{name}"
  puts "=" * 78
  load path
end
