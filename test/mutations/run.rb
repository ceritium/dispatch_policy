# frozen_string_literal: true

require "fileutils"
require "open3"
require "tmpdir"
require_relative "catalogue"

module DispatchPolicy
  module Mutations
    # Runs the catalogue: copy the tree, break one line, run the suite,
    # restore, repeat. Green suite on broken code = the line is unguarded.
    #
    # Four outcomes, and the distinction between the last two is the whole
    # point of the runner:
    #
    #   CAUGHT     the suite RAN and failed. The line is guarded.
    #   SURVIVED   the suite ran and passed on broken code. Nothing guards it.
    #   NO TARGET  the `find` string is not in the file any more. The
    #              mutation is stale and proves NOTHING — the code moved
    #              under it.
    #   INVALID    the mutation produced a file that does not parse. Proves
    #              nothing: the suite never ran.
    #   NO RESULT  the suite produced no summary line at all — it could not
    #              boot, bundler failed, the database was gone, it hung.
    #              Also proves nothing, and this is the one that hides: a
    #              non-green exit looks exactly like a catch from outside.
    #   UNATTRIBUTED
    #              the suite failed, but not in the test the entry names.
    #              Something failed; we cannot say this mutation is why.
    #
    # The last three fail the run, and they exist because this project
    # shipped the opposite three times. A mutation of the hint struct was
    # mis-typed into a syntax error; the suite could not boot, the runner
    # read "not green" as "caught", and a line everyone believed was
    # covered was not — the same line that later 500'd the dashboard.
    # CAUGHT therefore requires a parsed summary line saying what failed,
    # AND that the failure be in the test the entry says should notice.
    # Without that last check a CAUGHT means only "something was red":
    # a leaked `idle in transaction` backend on a shared database once
    # made an unrelated mutation fail 25 tests and score CAUGHT, and a
    # stale `caught_by` can point at a test that has not run in months
    # while the line it names is actually unguarded.
    # Never let a mutation that did not actually run count as a pass.
    module Runner
      module_function

      DB = ENV.fetch("MUTATION_DB", "dispatch_policy_mutations")

      # A mutation can deadlock the suite instead of failing it. Without a
      # bound the whole run hangs with no output and no result.
      TIMEOUT = Integer(ENV.fetch("MUTATION_TIMEOUT", "600"))

      # Copied rather than mutated in place: an interrupt mid-run would
      # otherwise leave the working tree broken in a way `git status`
      # makes look deliberate.
      SKIP = [".git", "tmp", "log", ".bundle"].freeze

      def run(filter: ENV["FILTER"])
        selected = select(filter)
        abort "no mutation matches FILTER=#{filter}" if selected.empty?

        Dir.mktmpdir("dispatch_policy_mutations") do |tmp|
          tree = File.join(tmp, "tree")
          copy_tree(tree)
          ensure_database

          say "control run (#{selected.size} mutation(s) selected)"
          control = suite(tree)
          unless control[:ran] && control[:green]
            abort "the control run is not usable (#{control[:summary]}). " \
                  "Fix the suite before reading anything into a mutation."
          end
          say "control: #{control[:summary]}"
          puts

          report(selected.map { |m| [m, apply(tree, m)] })
        end
      end

      def select(filter)
        return ALL if filter.nil? || filter.empty?

        ALL.select { |m| m[:id] == filter || m[:label].include?(filter) || m[:file].include?(filter) }
      end

      def apply(tree, mutation)
        path     = File.join(tree, mutation[:file])
        original = File.read(path)

        unless original.include?(mutation[:find])
          return { outcome: :no_target }
        end

        File.write(path, original.sub(mutation[:find], mutation[:replace]))
        begin
          syntax = Open3.capture3("ruby", "-c", path) if path.end_with?(".rb")
          if syntax && !syntax[2].success?
            return { outcome: :invalid, detail: syntax[1].lines.first.to_s.strip.sub(%r{\A\S+\.rb:}, "line ") }
          end

          result = suite(tree)
          if !result[:ran]
            { outcome: :no_result, detail: result[:summary] }
          elsif result[:green]
            { outcome: :survived }
          elsif attributed?(mutation, result[:failed])
            { outcome: :caught, detail: "#{result[:failed].join(', ')} — #{result[:summary]}" }
          else
            { outcome: :unattributed,
              detail: "failed in #{result[:failed].join(', ')}, not in #{mutation[:caught_by]}" }
          end
        ensure
          File.write(path, original)
        end
      end

      # A CAUGHT has to be caught by the test the entry NAMES. Minitest
      # prints the class; `caught_by` names files, so compare on the
      # snake_cased class.
      def attributed?(mutation, failed_classes)
        named = mutation[:caught_by].to_s.split(",").map(&:strip).reject(&:empty?)
        return false if named.empty? || named.first.start_with?("none")

        failed_classes.any? { |klass| named.any? { |n| snake(klass).include?(n) } }
      end

      def snake(class_name)
        class_name.split("::").last.to_s
                  .gsub(/([a-z\d])([A-Z])/, '\1_\2')
                  .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
                  .downcase
      end

      # `Bundler.with_unbundled_env` matters: without it BUNDLE_GEMFILE
      # still points at the real checkout, so the copy runs the ORIGINAL
      # code and every mutation "survives".
      def suite(tree)
        out = +""
        timed_out = false
        Bundler.with_unbundled_env do
          Open3.popen2e({ "DB_NAME" => DB, "DISPATCH_POLICY_REQUIRE_DB" => "1" },
                        "bundle", "exec", "rake", "test", chdir: tree) do |stdin, oe, wait|
            stdin.close
            reader = Thread.new { out << oe.read }
            if wait.join(TIMEOUT)
              reader.join
            else
              timed_out = true
              begin
                Process.kill("KILL", wait.pid)
              rescue StandardError
                nil
              end
              reader.kill
            end
          end
        end
        return { ran: false, green: false, summary: "timed out after #{TIMEOUT}s" } if timed_out

        line = out[/^\d+ runs, \d+ assertions, \d+ failures, \d+ errors.*$/]
        unless line
          # No summary line means minitest never finished, so there is no
          # evidence either way. Carry the tail out so the operator can
          # see WHY without re-running by hand.
          return { ran: false, green: false,
                   summary: "no summary line: #{out.lines.last(2).join(' ').strip[0, 120]}" }
        end

        failures, errors = line.scan(/(\d+) failures, (\d+) errors/).flatten.map(&:to_i)
        failed = out.scan(/^(?:Failure|Error):\n\s*([A-Za-z0-9_:]+)#test_/).flatten.uniq
        { ran: true, green: failures.zero? && errors.zero?, summary: line, failed: failed }
      end

      def copy_tree(tree)
        FileUtils.mkdir_p(tree)
        Dir.children(root).each do |entry|
          next if SKIP.include?(entry)

          FileUtils.cp_r(File.join(root, entry), File.join(tree, entry))
        end
      end

      def ensure_database
        Open3.capture3("createdb", DB) # already there is fine
      end

      def root
        File.expand_path("../..", __dir__)
      end

      def say(message)
        puts "[mutations] #{message}"
      end

      def report(results)
        results.each do |mutation, result|
          puts format("  %-4s %-10s %s%s", mutation[:id], banner(result[:outcome]),
                      mutation[:label],
                      result[:detail] ? "  (#{result[:detail]})" : "")
        end

        unexpected = results.select do |mutation, result|
          case result[:outcome]
          when :survived then !EXPECTED_SURVIVORS.key?(mutation[:id])
          when :no_target, :invalid, :no_result, :unattributed then true
          else false
          end
        end
        stale = results.select do |mutation, result|
          result[:outcome] == :caught && EXPECTED_SURVIVORS.key?(mutation[:id])
        end

        puts
        EXPECTED_SURVIVORS.each do |id, reason|
          next unless results.any? { |m, r| m[:id] == id && r[:outcome] == :survived }

          puts "  #{id} survives as expected: #{reason}"
        end

        stale.each do |mutation, _|
          puts "  #{mutation[:id]} is listed as an expected survivor but was CAUGHT — " \
               "the note is stale, drop it from EXPECTED_SURVIVORS."
        end

        if unexpected.empty?
          puts "\n#{results.size} mutation(s), every one accounted for."
          exit(stale.empty? ? 0 : 1)
        end

        puts "\n#{unexpected.size} unaccounted-for mutation(s):"
        unexpected.each do |mutation, result|
          puts "  - #{mutation[:id]} #{mutation[:label]} [#{result[:outcome]}]"
          puts "    should be caught by: #{mutation[:caught_by]}"
        end
        puts "\nA survivor means the test is decorative: fix the test, not the catalogue."
        exit 1
      end

      def banner(outcome)
        { caught: "CAUGHT", survived: "SURVIVED", no_target: "NO TARGET",
          invalid: "INVALID", no_result: "NO RESULT",
          unattributed: "UNATTRIB" }.fetch(outcome)
      end
    end
  end
end
