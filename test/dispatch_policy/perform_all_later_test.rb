# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class PerformAllLaterTest < ActiveSupport::TestCase
    class BulkJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        dedupe_key ->(args) { "bulk:#{args.first}" }
      end
      def perform(*); end
    end

    test "perform_all_later stages every dispatchable job in one go" do
      jobs = %w[a b c d e].map { |k| BulkJob.new(k) }

      assert_difference -> { StagedJob.pending.count }, +5 do
        ActiveJob.perform_all_later(jobs)
      end
    end

    test "perform_all_later drops duplicate dedupe keys" do
      BulkJob.perform_later("a")
      dups = 3.times.map { BulkJob.new("a") }

      assert_no_difference -> { StagedJob.count } do
        ActiveJob.perform_all_later(dups)
      end
    end
  end
end
