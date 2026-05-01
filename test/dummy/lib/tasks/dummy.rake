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

  desc "Kick off the dispatch tick loop in the dummy"
  task dummy_tick: :environment do
    DispatchTickLoopJob.perform_later
    Rails.logger.info("[dummy] DispatchTickLoopJob enqueued; the worker will pick it up.")
  end

  desc "Reset dummy database, migrate, and seed nothing"
  task dummy_setup: :environment do
    sh = ->(cmd) { puts "$ #{cmd}"; system(cmd) || abort("failed: #{cmd}") }
    sh.call("bin/rails db:drop db:create RAILS_ENV=development")
    sh.call("bin/rails db:migrate RAILS_ENV=development")
  end
end
