# frozen_string_literal: true

require_relative "../test_helper"

class CursorPaginationTest < Minitest::Test
  CP = DispatchPolicy::CursorPagination

  def test_encode_and_decode_roundtrip
    cursor = CP.encode(42, 100)
    assert_equal [42, 100], CP.decode(cursor)
  end

  def test_decode_garbage_returns_nil
    assert_nil CP.decode("not-base64-junk")
    assert_nil CP.decode("")
    assert_nil CP.decode(nil)
  end

  def test_decode_wrong_shape_returns_nil
    bad = Base64.urlsafe_encode64(JSON.dump({a: 1}), padding: false)
    assert_nil CP.decode(bad)
  end

  def test_serialize_value_normalizes_time_to_iso8601
    t = Time.utc(2026, 1, 15, 10, 30, 45)
    assert_match(/2026-01-15T10:30:45\.000000Z/, CP.serialize_value(t))
  end

  def test_sort_for_unknown_falls_back_to_default
    sort = CP.sort_for("nope")
    assert_equal CP.sort_for(CP::DEFAULT_SORT), sort
  end

  def test_apply_no_cursor_passes_scope_through
    fake_scope = Object.new
    assert_same fake_scope, CP.apply(fake_scope, "pending", nil)
  end
end
