# frozen_string_literal: true

require "test_helper"

module DispatchPolicy
  class PolicyConfigTest < ActiveSupport::TestCase
    class LiveTunedJob < ActiveJob::Base
      include DispatchPolicy::Dispatchable
      dispatch_policy do
        round_robin_by ->(args) { args.first }
        batch_size          250
        round_robin_quantum 5
      end
      def perform(*); end
    end

    setup do
      @policy_name = LiveTunedJob.resolved_dispatch_policy.name
      @original_source    = DispatchPolicy.config.policy_config_source
      @original_auto_tune = DispatchPolicy.config.auto_tune
    end

    teardown do
      DispatchPolicy.config.policy_config_source = @original_source
      DispatchPolicy.config.auto_tune            = @original_auto_tune
      # Restore DSL ivars in case a test mutated them.
      policy = LiveTunedJob.resolved_dispatch_policy
      policy.instance_variable_set(:@override_batch_size, 250)
      policy.instance_variable_set(:@override_round_robin_quantum, 5)
      policy.instance_variable_set(:@override_round_robin_max_partitions_per_tick, nil)
      policy.instance_variable_set(:@override_lease_duration, nil)
    end

    # ─── load_into / persist roundtrip ────────────────────────────

    test "load_into reads DB rows into matching @override_X ivars" do
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "batch_size",
        value: "1000", source: "ui"
      )
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "round_robin_quantum",
        value: "1", source: "ui"
      )

      applied = PolicyConfig.load_into(LiveTunedJob.resolved_dispatch_policy)

      assert_equal({ batch_size: 1000, round_robin_quantum: 1 }, applied)
      assert_equal 1000, LiveTunedJob.resolved_dispatch_policy.effective_batch_size
      assert_equal 1,    LiveTunedJob.resolved_dispatch_policy.effective_round_robin_quantum
    end

    test "load_into ignores unknown keys (forward-compat)" do
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "invented_future_knob",
        value: "42", source: "ui"
      )
      assert_nothing_raised do
        PolicyConfig.load_into(LiveTunedJob.resolved_dispatch_policy)
      end
    end

    test "persist_overrides! writes current overrides to DB" do
      policy = LiveTunedJob.resolved_dispatch_policy
      policy.batch_size 1500
      policy.persist_overrides!(source: "ui")

      row = PolicyConfig.find_by(policy_name: @policy_name, config_key: "batch_size")
      assert_equal "1500", row.value
      assert_equal "ui",   row.source
    end

    test "upsert_many! is a no-op when value matches existing row" do
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "batch_size",
        value: "250", source: "ui"
      )
      original_updated_at = PolicyConfig
        .find_by(policy_name: @policy_name, config_key: "batch_size").updated_at

      sleep 0.01
      changed = PolicyConfig.upsert_many!(
        policy_name: @policy_name,
        values:      { batch_size: 250 },
        source:      "auto"
      )

      assert_equal 0, changed
      reloaded = PolicyConfig.find_by(policy_name: @policy_name, config_key: "batch_size")
      assert_equal original_updated_at.to_i, reloaded.updated_at.to_i
      assert_equal "ui", reloaded.source
    end

    # ─── reload_overrides_from_db! mode semantics ────────────────

    test ":db mode — DB rows override the DSL on reload" do
      DispatchPolicy.config.policy_config_source = :db
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "batch_size",
        value: "777", source: "ui"
      )

      LiveTunedJob.resolved_dispatch_policy.reload_overrides_from_db!

      assert_equal 777, LiveTunedJob.resolved_dispatch_policy.effective_batch_size
    end

    test ":code mode — DSL mirrors back into DB and stays authoritative" do
      DispatchPolicy.config.policy_config_source = :code
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "batch_size",
        value: "999", source: "ui"
      )

      LiveTunedJob.resolved_dispatch_policy.reload_overrides_from_db!

      assert_equal 250, LiveTunedJob.resolved_dispatch_policy.effective_batch_size,
                   "DSL value should still win"
      row = PolicyConfig.find_by(policy_name: @policy_name, config_key: "batch_size")
      assert_equal "250",  row.value
      assert_equal "code", row.source
    end

    # ─── TickLoop auto_tune integration ──────────────────────────

    # Set up a real bottleneck so Stats.bottleneck returns rotation_lag
    # without having to stub the module method (Stats uses
    # module_function, and stubbing those is awkward without Mocha).
    def stage_lru_lagged_partitions(count: 3, threshold_seconds: 60)
      now = Time.current
      old = now - (threshold_seconds * 2).seconds
      count.times do |i|
        PartitionState.create!(
          policy_name:      @policy_name,
          partition_key:    "tenant-#{i}",
          pending_count:    1,
          last_admitted_at: old,
          created_at:       old,
          updated_at:       old
        )
        StagedJob.create!(
          policy_name:      @policy_name,
          job_class:        "LiveTunedJob",
          arguments:        [],
          dedupe_key:       "k-#{i}-#{Time.now.to_f}",
          round_robin_key:  "tenant-#{i}",
          staged_at:        old
        )
      end
    end

    test "TickLoop.maybe_auto_tune! :apply persists Stats.bottleneck recommendations" do
      DispatchPolicy.config.auto_tune = :apply
      stage_lru_lagged_partitions

      TickLoop.maybe_auto_tune!(@policy_name)

      row = PolicyConfig.find_by(policy_name: @policy_name, config_key: "round_robin_quantum")
      refute_nil row, "expected an auto-tune row to be persisted"
      assert_equal "1",    row.value
      assert_equal "auto", row.source
      assert_equal 1, LiveTunedJob.resolved_dispatch_policy.effective_round_robin_quantum
    end

    test "TickLoop.maybe_auto_tune! :recommend does not write to DB" do
      DispatchPolicy.config.auto_tune = :recommend
      stage_lru_lagged_partitions

      TickLoop.maybe_auto_tune!(@policy_name)

      assert_nil PolicyConfig.find_by(policy_name: @policy_name, config_key: "round_robin_quantum")
    end

    test "TickLoop.maybe_auto_tune! false is a no-op" do
      DispatchPolicy.config.auto_tune = false

      TickLoop.maybe_auto_tune!(@policy_name)

      assert_equal 0, PolicyConfig.where(policy_name: @policy_name).count
    end

    test "TickLoop.reload_policy_configs! applies DB values for one policy" do
      DispatchPolicy.config.policy_config_source = :db
      PolicyConfig.create!(
        policy_name: @policy_name, config_key: "batch_size",
        value: "333", source: "ui"
      )

      TickLoop.reload_policy_configs!(@policy_name)

      assert_equal 333, LiveTunedJob.resolved_dispatch_policy.effective_batch_size
    end
  end
end
