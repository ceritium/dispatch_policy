# frozen_string_literal: true

require_relative "../test_helper"

# `Forwarder#deserialize!` decides what gets HELD. Anything it lets
# through escapes to `Tick`'s generic rescue, which queues a backoff and
# writes no `failed_at` — so nothing releases the row and it heads every
# subsequent claim of that partition forever, healthy neighbours
# included. This rescue has been narrowed and reverted twice.
#
# The integration tests each stage one concrete failure, so a rescue that
# lists exactly those classes walks past all of them while leaving every
# other error wedging a partition. Pin the RULE instead: an error class
# nobody could have enumerated, because it does not exist until this test
# defines it.
class ForwarderDeserializeTest < Minitest::Test
  def test_an_unforeseeable_error_out_of_deserialize_is_still_held
    boom = Class.new(StandardError)
    klass = Class.new do
      define_singleton_method(:deserialize) { |_| raise boom, "from a host we cannot see" }
    end
    Object.const_set(:ForwarderDeserializeProbeJob, klass)

    begin
      err = assert_raises(DispatchPolicy::UndeliverableJob) do
        DispatchPolicy::Forwarder.deserialize!(
          "id" => 42, "job_class" => "ForwarderDeserializeProbeJob",
          "job_data" => { "job_class" => "ForwarderDeserializeProbeJob" }
        )
      end

      assert_equal [42], err.staged_ids,
                   "the caller quarantines exactly these ids; without them the whole " \
                   "batch is held instead of the one bad row"
      assert_match(/from a host we cannot see/, err.message,
                   "the reason column is the only thing the operator has to act on")
    ensure
      Object.send(:remove_const, :ForwarderDeserializeProbeJob)
    end
  end
end
