# frozen_string_literal: true

# Run EXPLAIN (ANALYZE, BUFFERS) on every SQL that Dispatch.run fires,
# at chosen partition scales. Output is the literal Postgres plan, so
# you can see where the time goes (planning, sequential scans, sort
# spills) and decide whether an index or a query restructure would
# help.
#
#   bundle exec rake test:profile
#   SCALES=10000,100000 bundle exec rake test:profile

require_relative "benchmark_helper"

include DispatchPolicy::BenchmarkHelpers

class ProfileConcurrencyJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    partition_by ->(args) { args.first }
    concurrency  max: 1_000_000
  end
  def perform(*); end
end

class ProfileThrottleJob < ActiveJob::Base
  include DispatchPolicy::Dispatchable
  dispatch_policy do
    partition_by ->(args) { args.first }
    throttle     rate: 1_000_000, per: 1.second, burst: 1_000_000
  end
  def perform(*); end
end

DispatchPolicy.config.batch_size = (ENV["BATCH_SIZE"] || 500).to_i

SCALES             = (ENV["SCALES"]             || "10000,100000").split(",").map(&:to_i).freeze
JOBS_PER_PARTITION = (ENV["JOBS_PER_PARTITION"] || 3).to_i

pg_version = ActiveRecord::Base.connection.execute("SHOW server_version").first["server_version"]

puts ""
puts "═" * 72
puts " DispatchPolicy dispatch profiler"
puts "═" * 72
puts "  scales:        #{SCALES.map { |n| fmt_int(n) }.join(', ')}"
puts "  batch_size:    #{DispatchPolicy.config.batch_size}"
puts "  jobs/part:     #{JOBS_PER_PARTITION}"
puts "  postgres:      #{pg_version}"

# Capture every SQL fired by `block`, filtering to dispatcher tables.
def capture_sql
  rows = []
  sub = ActiveSupport::Notifications.subscribe("sql.active_record") do |_name, started, finished, _id, payload|
    next if payload[:name] == "SCHEMA"
    sql = payload[:sql]
    next unless sql.include?("dispatch_policy_staged_jobs") ||
                sql.include?("dispatch_policy_partitions")
    next if /\A\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)/i.match?(sql)
    rows << {
      name:        (payload[:name] || "").to_s,
      sql:         sql,
      binds:       payload[:type_casted_binds] || [],
      duration_ms: (finished - started) * 1000.0
    }
  end
  yield
  ActiveSupport::Notifications.unsubscribe(sub)
  rows
end

def inline_binds(sql, binds)
  return sql if binds.nil? || binds.empty?
  conn = ActiveRecord::Base.connection
  inlined = sql.dup
  values = binds.map { |b| b.respond_to?(:call) ? b.call : b }
  values.each_with_index.to_a.reverse_each do |value, i|
    inlined.gsub!("$#{i + 1}", conn.quote(value))
  end
  inlined
end

def explain(sql, binds)
  return nil if /\A\s*(UPDATE|INSERT|DELETE)/i.match?(sql)
  inlined = inline_binds(sql, binds)
  ActiveRecord::Base.connection
    .execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) #{inlined}")
    .map { |r| r["QUERY PLAN"] }
    .join("\n")
rescue ActiveRecord::StatementInvalid => e
  "  [EXPLAIN failed: #{e.message.lines.first&.strip}]"
end

def truncate_summary(sql, limit: 240)
  one_line = sql.gsub(/\s+/, " ").strip
  one_line.length > limit ? "#{one_line[0, limit]}…" : one_line
end

def profile(label, job_class)
  policy = job_class.resolved_dispatch_policy

  SCALES.each do |n|
    truncate_all!
    seed_with_progress(
      "#{label} N=#{fmt_int(n)}",
      job_class:          job_class,
      partitions:         n,
      jobs_per_partition: JOBS_PER_PARTITION
    )

    if ENV["ANALYZE_AFTER_SEED"] != "0"
      status("#{label} N=#{fmt_int(n)}: ANALYZE…")
      ActiveRecord::Base.connection.execute("ANALYZE dispatch_policy_staged_jobs")
      ActiveRecord::Base.connection.execute("ANALYZE dispatch_policy_partitions")
    end

    status("#{label} N=#{fmt_int(n)}: capturing SQL…")
    captured = capture_sql { DispatchPolicy::Dispatch.run(policy_name: policy.name) }
    clear_status

    print_section("#{label} — N=#{fmt_int(n)}")
    captured.each_with_index do |q, i|
      puts ""
      puts "Q#{i + 1}: #{q[:name]} — measured #{format('%.1f', q[:duration_ms])} ms"
      puts "  #{truncate_summary(q[:sql])}"
      plan = explain(q[:sql], q[:binds])
      next unless plan
      puts ""
      plan.each_line { |line| puts "    #{line}" }
    end
  end
end

profile("dispatch — concurrency", ProfileConcurrencyJob)
profile("dispatch — throttle",    ProfileThrottleJob)

truncate_all!
puts ""
puts "Done."
puts ""
