# frozen_string_literal: true

module DispatchPolicy
  # DB-backed per-policy config overrides. Loaded into Policy
  # instances at TickLoop startup so cluster-wide tunes take effect
  # within tick_max_duration without redeploying.
  #
  # Stored as text; decoded into the same Ruby type the corresponding
  # DispatchPolicy.config knob uses (Integer for batch_size /
  # round_robin_quantum / round_robin_max_partitions_per_tick /
  # lease_duration). Unknown keys are ignored on load — forward-
  # compatible with future knobs.
  class PolicyConfig < ApplicationRecord
    self.table_name = "dispatch_policy_policy_configs"

    KNOWN_KEYS = %i[
      batch_size
      round_robin_quantum
      round_robin_max_partitions_per_tick
      lease_duration
    ].freeze

    # Read every row for `policy_name` into the matching @override_X
    # ivars on `policy`. Returns the Hash that was applied.
    def self.load_into(policy)
      rows = where(policy_name: policy.name).pluck(:config_key, :value)
      applied = {}
      rows.each do |key, raw|
        sym = key.to_sym
        next unless KNOWN_KEYS.include?(sym)
        decoded = decode(raw)
        next if decoded.nil?
        policy.instance_variable_set(:"@override_#{sym}", decoded)
        applied[sym] = decoded
      end
      applied
    end

    # Bulk-upsert a Hash of { key => value } for one policy. Skips
    # writes when the existing row already matches (so auto-tune
    # doesn't churn updated_at on every tick).
    def self.upsert_many!(policy_name:, values:, source: "ui", now: Time.current)
      return 0 if values.blank?

      existing = where(policy_name: policy_name, config_key: values.keys.map(&:to_s))
        .pluck(:config_key, :value)
        .to_h

      changed = values.reject { |k, v| existing[k.to_s] == v.to_s }
      return 0 if changed.empty?

      rows = changed.map do |key, val|
        {
          policy_name: policy_name,
          config_key:  key.to_s,
          value:       val.to_s,
          source:      source,
          created_at:  now,
          updated_at:  now
        }
      end

      upsert_all(rows, unique_by: %i[policy_name config_key])
      changed.size
    end

    def self.decode(raw)
      return nil if raw.nil? || raw.to_s.strip.empty?
      Integer(raw)
    rescue ArgumentError, TypeError
      begin
        Float(raw)
      rescue ArgumentError, TypeError
        nil
      end
    end
    private_class_method :decode
  end
end
