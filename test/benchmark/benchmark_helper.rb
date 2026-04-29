# frozen_string_literal: true

# Shared bootstrap for the dispatch_policy benchmark scripts. Pulls in
# the regular test_helper to get the dummy Rails app, AR connection,
# and migrated schema, then exposes a few helpers for seeding and
# measuring. NOT a Minitest file — the benchmarks are scripts that
# print numbers to stdout, not pass/fail tests.

ENV["RAILS_ENV"] = "test"

# Skip SimpleCov for benchmarks — we don't want coverage instrumentation
# to skew numbers.
ENV["SIMPLECOV_DISABLED"] = "1" unless ENV.key?("SIMPLECOV_DISABLED")

require_relative "../test_helper"

module DispatchPolicy
  module BenchmarkHelpers
    module_function

    TABLES_IN_TRUNCATE_ORDER = %w[
      dispatch_policy_staged_jobs
      dispatch_policy_partition_counts
      dispatch_policy_partition_states
      dispatch_policy_throttle_buckets
      dispatch_policy_adaptive_concurrency_stats
      dispatch_policy_partition_observations
    ].freeze

    def truncate_all!
      conn = DispatchPolicy::ApplicationRecord.connection
      conn.execute("TRUNCATE TABLE #{TABLES_IN_TRUNCATE_ORDER.join(', ')} RESTART IDENTITY")
      adapter = ActiveJob::Base.queue_adapter
      adapter.enqueued_jobs.clear if adapter.respond_to?(:enqueued_jobs)
    end

    # Convenience wrapper: seed_staged! with a progress bar drawn on
    # stderr that erases itself when seeding finishes.
    def seed_with_progress(label, **kwargs)
      total = kwargs[:partitions] * kwargs[:jobs_per_partition]
      bar = ProgressBar.new("#{label}: seeding", total)
      seeded = seed_staged!(**kwargs) { |so_far| bar.update(so_far) }
      bar.finish!
      seeded
    end

    # Bulk-stage `partitions × jobs_per_partition` rows for a given job
    # class. Uses the same code path as production (StagedJob.stage_many!)
    # but in chunks so we don't hold every ActiveJob instance in memory
    # at once for very large N. Yields the running total after each
    # chunk so callers can drive a progress bar.
    def seed_staged!(job_class:, partitions:, jobs_per_partition:, chunk: 5_000)
      policy = job_class.resolved_dispatch_policy
      total  = 0

      partitions.times.each_slice(chunk / [ jobs_per_partition, 1 ].max) do |slice|
        jobs = []
        slice.each do |i|
          jobs_per_partition.times { jobs << job_class.new("p#{i}") }
        end
        DispatchPolicy::StagedJob.stage_many!(policy: policy, jobs: jobs)
        total += jobs.size
        yield(total) if block_given?
      end

      total
    end

    # Run `block`, returning [wall_ms, sql_count, result]. SQL count
    # excludes SCHEMA queries (introspection done by AR on first
    # contact with a table).
    def measure
      sql_count = 0
      sub = ActiveSupport::Notifications.subscribe("sql.active_record") do |*, payload|
        next if payload[:name] == "SCHEMA"
        sql_count += 1
      end

      result = nil
      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = yield
      wall = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

      ActiveSupport::Notifications.unsubscribe(sub)
      [ wall * 1000.0, sql_count, result ]
    end

    # ─── Output formatting ─────────────────────────────────────────

    def fmt_int(n)
      n.to_i.to_s.reverse.scan(/.{1,3}/).join(",").reverse
    end

    def fmt_ms(n)
      n >= 1000 ? fmt_int(n.round) : format("%.1f", n)
    end

    # Print a column-aligned table that's still valid markdown. Numeric
    # columns are right-aligned (header gets a `---:` separator). Pads
    # every cell to the column's max width so the result reads cleanly
    # in a fixed-width terminal AND renders nicely in markdown viewers.
    #
    #   align: array of :left or :right per column (defaults to :right
    #   for every column except the first).
    def print_table(headers, rows, align: nil)
      align ||= Array.new(headers.size, :right)
      cols = headers.size
      widths = (0...cols).map do |i|
        cell_widths = rows.map { |r| r[i].to_s.length }
        [ headers[i].to_s.length, *cell_widths ].max
      end

      pad = ->(text, i) {
        align[i] == :right ? text.to_s.rjust(widths[i]) : text.to_s.ljust(widths[i])
      }

      header_row = "| " + headers.each_with_index.map { |h, i| pad.call(h, i) }.join(" | ") + " |"
      sep_row    = "|" + widths.each_with_index.map { |w, i|
        align[i] == :right ? "-" * (w + 1) + ":" : ":" + "-" * (w + 1)
      }.join("|") + "|"

      puts header_row
      puts sep_row
      rows.each do |r|
        puts "| " + r.each_with_index.map { |c, i| pad.call(c, i) }.join(" | ") + " |"
      end
    end

    BAR = ("─" * 72).freeze

    def print_section(title)
      puts ""
      puts BAR
      puts " #{title}"
      puts BAR
    end

    # ─── Progress (stderr) ─────────────────────────────────────────
    #
    # Drawn over a single line that gets erased on `finish`. Uses
    # stderr so stdout (the report) stays clean — `rake test:benchmark
    # > report.md` captures only the markdown.

    PROGRESS_WIDTH = 32

    class ProgressBar
      def initialize(label, total, io: $stderr)
        @label   = label
        @total   = total.to_i
        @io      = io
        @current = 0
        @start   = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        draw
      end

      def update(current)
        @current = current
        draw
      end

      def finish!
        @io.print("\r\e[2K")
        @io.flush
      end

      private

      def draw
        return unless @io.tty? || ENV["BENCHMARK_FORCE_PROGRESS"] == "1"
        pct = @total.zero? ? 0 : ((@current.to_f / @total) * 100).round
        filled = @total.zero? ? 0 : ((PROGRESS_WIDTH * @current.to_f) / @total).round.clamp(0, PROGRESS_WIDTH)
        bar = ("█" * filled) + ("░" * (PROGRESS_WIDTH - filled))
        elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - @start
        line = format(
          "  %s [%s] %3d%% (%s/%s) %.1fs",
          @label, bar, pct,
          DispatchPolicy::BenchmarkHelpers.fmt_int(@current),
          DispatchPolicy::BenchmarkHelpers.fmt_int(@total),
          elapsed
        )
        @io.print("\r\e[2K#{line}")
        @io.flush
      end
    end

    # Single-line status (no bar) for phases without natural progress
    # updates — measuring a single op, prepping admit state, etc.
    def status(text, io: $stderr)
      return unless io.tty? || ENV["BENCHMARK_FORCE_PROGRESS"] == "1"
      io.print("\r\e[2K  #{text}")
      io.flush
    end

    def clear_status(io: $stderr)
      return unless io.tty? || ENV["BENCHMARK_FORCE_PROGRESS"] == "1"
      io.print("\r\e[2K")
      io.flush
    end
  end
end
