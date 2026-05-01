# frozen_string_literal: true

require_relative "../test_helper"

class SerializerTest < Minitest::Test
  class TinyJob < ActiveJob::Base
    def perform(_a, _b); end
  end

  def test_roundtrip
    job = TinyJob.new(1, "two")
    payload = DispatchPolicy::Serializer.serialize(job)

    assert_equal "SerializerTest::TinyJob", payload["job_class"]
    assert_equal [1, "two"], payload["arguments"]

    restored = DispatchPolicy::Serializer.deserialize(payload)
    assert_instance_of TinyJob, restored
    assert_equal job.job_id, restored.job_id
    # ActiveJob holds args in serialized form until #perform_now runs them; the
    # important property is that `restored.serialize` produces the same payload
    # as the original — that's what the real adapter receives when we re-enqueue.
    assert_equal [1, "two"], restored.serialize["arguments"]
  end

  def test_load_jsonb_handles_string_or_hash
    assert_equal({ "a" => 1 }, DispatchPolicy::Serializer.load_jsonb('{"a":1}'))
    assert_equal({ "a" => 1 }, DispatchPolicy::Serializer.load_jsonb({ "a" => 1 }))
    assert_equal({}, DispatchPolicy::Serializer.load_jsonb(nil))
  end
end
