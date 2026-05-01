# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class PolicyTest < ActiveSupport::TestCase
    class CappedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        concurrency  max: 3
      end
      def perform(*); end
    end

    class ThrottledJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        partition_by ->(args) { args.first }
        throttle     rate: 60, per: 60.seconds, burst: 60
      end
      def perform(*); end
    end

    test "DSL captures concurrency_max" do
      assert_equal 3, CappedJob.resolved_dispatch_policy.concurrency_max
    end

    test "DSL captures throttle rate / burst" do
      p = ThrottledJob.resolved_dispatch_policy
      assert_in_delta 1.0, p.throttle_rate, 0.001  # 60/60s = 1 token/sec
      assert_equal   60,   p.throttle_burst
    end

    test "concurrency without partition_by raises" do
      assert_raises ArgumentError do
        Class.new(ActiveJob::Base) do
          include DispatchPolicy::Dispatchable
          def self.name; "Anon::A"; end
          dispatch_policy { concurrency max: 1 }
          def perform(*); end
        end
      end
    end

    test "throttle rate must be positive" do
      assert_raises ArgumentError do
        Class.new(ActiveJob::Base) do
          include DispatchPolicy::Dispatchable
          def self.name; "Anon::B"; end
          dispatch_policy do
            partition_by ->(args) { args.first }
            throttle rate: 0, per: 1.second
          end
          def perform(*); end
        end
      end
    end

    test "concurrency max must be Integer (not Proc)" do
      assert_raises ArgumentError do
        Class.new(ActiveJob::Base) do
          include DispatchPolicy::Dispatchable
          def self.name; "Anon::C"; end
          dispatch_policy do
            partition_by ->(args) { args.first }
            concurrency  max: ->(_ctx) { 5 }
          end
          def perform(*); end
        end
      end
    end
  end
end
