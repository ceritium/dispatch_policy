# frozen_string_literal: true

require_relative "../test_helper"

# `warn_split_connection` is the whole shipped mitigation for a silent
# at-least-once loss: a PG-backed adapter writing through a different
# connection class than the gem opens its transaction on means the
# adapter's INSERT does not join that transaction. It had no test, and it
# could never fire for good_job — the adapter this project defaults to —
# because good_job writes through GoodJob::BaseRecord and defines no
# `:Record` anywhere on its adapter. Its name is in the PG-backed hint
# list, so the generic warning was skipped too: boot was silent.
class AdapterWarningTest < Minitest::Test
  class Logger
    attr_reader :warnings
    def initialize = @warnings = []
    def warn(msg) = @warnings << msg
  end

  module FakeGoodJob
    class BaseRecord
      def self.connection_specification_name = "GoodJobSpec"
    end
    class Adapter; end
  end

  def setup
    super
    @logger = Logger.new
    DispatchPolicy.config.logger = @logger
  end

  def teardown
    DispatchPolicy.reset_config!
  end

  def warn_for(adapter)
    DispatchPolicy.warn_unsupported_adapter_for(adapter)
    @logger.warnings
  end

  def test_it_warns_when_the_adapter_writes_through_another_connection
    Object.const_set(:GoodJob, FakeGoodJob) unless defined?(::GoodJob)
    begin
      warnings = warn_for(FakeGoodJob::Adapter.new)
      assert warnings.any? { |w| w.include?("GoodJobSpec") || w.include?("BaseRecord") },
             "silent boot is how a split connection loses jobs unnoticed: #{warnings.inspect}"
    ensure
      Object.send(:remove_const, :GoodJob) if defined?(::GoodJob) && ::GoodJob.equal?(FakeGoodJob)
    end
  end

  def test_a_plain_top_level_Record_constant_cannot_abort_boot
    Object.const_set(:Record, Struct.new(:x))
    begin
      warn_for(Object.new) # no adapter hint matches; must not raise
      pass
    ensure
      Object.send(:remove_const, :Record)
    end
  end
end
