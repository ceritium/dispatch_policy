# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class PoliciesControllerTest < ActionDispatch::IntegrationTest
    include Engine.routes.url_helpers

    class SampleJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        context ->(args) { { tenant: args.first } }
        gate :concurrency, max: 1, partition_by: ->(ctx) { ctx[:tenant] }
      end
      def perform(*); end
    end

    test "index lists registered policies" do
      SampleJob.perform_later("A")
      get "/dispatch_policy"
      assert_response :success
      assert_match(/dispatch_policy-policies_controller_test-sample_job/, response.body)
    end

    test "show renders policy details" do
      SampleJob.perform_later("A")
      policy_name = SampleJob.resolved_dispatch_policy.name
      get "/dispatch_policy/policies/#{policy_name}"
      assert_response :success
      assert_match(/Pending/, response.body)
    end

    test "show returns 404 for unknown policy" do
      get "/dispatch_policy/policies/does-not-exist"
      assert_response :not_found
    end
  end
end
