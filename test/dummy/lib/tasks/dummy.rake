# frozen_string_literal: true

namespace :dispatch_policy do
  desc "Run the configured background-job worker for the dummy app"
  task dummy_worker: :environment do
    case (ENV["DUMMY_ADAPTER"] || "good_job")
    when "good_job"
      Rails.logger.info("[dummy] starting good_job worker")
      GoodJob::CLI.start(["start", "--queues=*", "--max-threads=5"])
    when "solid_queue"
      Rails.logger.info("[dummy] starting solid_queue worker")
      SolidQueue::Cli.new.invoke(:start)
    end
  end

  desc "Run the dispatch tick loop in-process (long-running; foreman-friendly)"
  task dummy_tick: :environment do
    # Eager-load the predefined jobs so their `dispatch_policy` macros register
    # in DispatchPolicy.registry before the tick loop starts looking for them.
    %w[SlowExternalApiJob BulkAccountJob MixedJob PerformInJob RetryFlakyJob].each(&:constantize)

    Rails.logger.info("[dummy] starting in-process tick loop (policies: #{DispatchPolicy.registry.names.inspect})")

    stop = false
    %w[INT TERM].each { |sig| trap(sig) { stop = true } }

    DispatchPolicy::TickLoop.run(stop_when: -> { stop })

    Rails.logger.info("[dummy] tick loop stopped")
  end

  desc "Reset dummy database, migrate, and seed nothing"
  task dummy_setup: :environment do
    sh = ->(cmd) { puts "$ #{cmd}"; system(cmd) || abort("failed: #{cmd}") }
    sh.call("bin/rails db:drop db:create RAILS_ENV=development")
    sh.call("bin/rails db:migrate RAILS_ENV=development")
  end
end
