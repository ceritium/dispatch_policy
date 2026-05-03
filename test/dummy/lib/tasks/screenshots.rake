# frozen_string_literal: true

namespace :screenshots do
  desc "Capture admin UI screenshots into <gem_root>/screenshots/"
  task capture: :environment do
    require "capybara"
    require "capybara/dsl"
    require "selenium-webdriver"
    require "fileutils"
    require "cgi"

    Capybara.register_driver(:headless_chrome_retina) do |app|
      options = Selenium::WebDriver::Chrome::Options.new
      options.add_argument("--headless=new")
      options.add_argument("--window-size=1440,900")
      options.add_argument("--force-device-scale-factor=2")
      options.add_argument("--no-sandbox")
      options.add_argument("--disable-gpu")
      options.add_argument("--disable-dev-shm-usage")
      Capybara::Selenium::Driver.new(app, browser: :chrome, options: options)
    end

    Capybara.default_driver        = :headless_chrome_retina
    Capybara.app                   = Rails.application
    Capybara.server                = :puma, { Silent: true }
    Capybara.default_max_wait_time = 10

    seed_admin_state

    out_dir = Rails.root.join("../../screenshots").expand_path
    FileUtils.mkdir_p(out_dir)

    session = Capybara::Session.new(:headless_chrome_retina, Rails.application)

    save = lambda do |name|
      path = out_dir.join("#{name}.png")
      session.save_screenshot(path)
      puts "  saved #{name}.png"
    end

    puts "Capturing admin pages..."

    session.visit "/dispatch_policy"
    save.call("admin-index")

    session.visit "/dispatch_policy/policies"
    save.call("policies-index")

    %w[fairness_demo adaptive_demo high_throttle high_concurrency mixed].each do |policy_name|
      next unless DispatchPolicy.registry.fetch(policy_name)

      session.visit "/dispatch_policy/policies/#{CGI.escape(policy_name)}"
      save.call("admin-policy-#{policy_name}")
    end

    session.visit "/dispatch_policy/partitions"
    save.call("partitions-index")

    session.quit
    puts "Done. Screenshots are in #{out_dir}."
  end

  # Stage jobs through DispatchPolicy, run ticks to admit some, drain
  # the GoodJob queue inline, then leave fresh pending state so the
  # admin UI shows realistic counters and sparklines.
  def seed_admin_state
    puts "Seeding admin state..."

    DispatchPolicy::StagedJob.delete_all
    DispatchPolicy::InflightJob.delete_all
    DispatchPolicy::Partition.delete_all
    DispatchPolicy::TickSample.delete_all
    DispatchPolicy::AdaptiveConcurrencyStats.delete_all if defined?(DispatchPolicy::AdaptiveConcurrencyStats)

    # Fairness: one tenant floods, four trickle.
    100.times { FairnessDemoJob.perform_later(tenant: "loud") }
    %w[quiet_a quiet_b quiet_c quiet_d].each do |tenant|
      8.times { FairnessDemoJob.perform_later(tenant: tenant) }
    end

    # Adaptive: a few tenants with bursts.
    %w[acc_1 acc_2 acc_3].each do |tenant|
      20.times { AdaptiveDemoJob.perform_later(tenant: tenant) }
    end

    # High throttle: spread across endpoints.
    %w[orders payments search profiles].each do |endpoint|
      15.times { HighThrottleJob.perform_later(endpoint: endpoint, rate: 60) }
    end

    # High concurrency: a few buckets.
    %w[heavy light].each do |bucket|
      30.times { HighConcurrencyJob.perform_later(bucket: bucket, max: 5) }
    end

    # Mixed: combined throttle + concurrency.
    %w[ep_a ep_b].each do |ep|
      %w[acct_1 acct_2 acct_3].each do |acct|
        6.times { MixedJob.perform_later("endpoint_id" => ep, "account_id" => acct) }
      end
    end

    drain_through_dispatch_policy

    # Leave a bit of fresh pending so partitions show "pending" > 0.
    20.times { FairnessDemoJob.perform_later(tenant: "loud") }
    %w[acc_1 acc_2].each { |t| 5.times { AdaptiveDemoJob.perform_later(tenant: t) } }
  end

  def drain_through_dispatch_policy
    policies = DispatchPolicy.registry.each.map(&:name)
    return if policies.empty?

    require "good_job" if defined?(GoodJob).!

    6.times do
      policies.each { |name| DispatchPolicy::Tick.run(policy_name: name) }
      GoodJob.perform_inline if defined?(GoodJob)
      sleep 0.05
    end
  end
end
