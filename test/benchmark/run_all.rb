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

# Skip bench_helper.rb (required by every bench_*.rb) and the
# real-adapter bench (boots the dummy Rails app and needs the dummy DB
# pre-seeded with good_job/solid_queue tables — runs separately via
# `rake bench:real`).
scripts = Dir[File.join(__dir__, "bench_*.rb")]
            .reject { |p| %w[bench_helper.rb bench_real_adapter.rb].include?(File.basename(p)) }
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
