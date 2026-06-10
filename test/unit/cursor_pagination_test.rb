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

  def test_decode_rejects_non_scalar_value_or_non_integer_id
    # Hostile cursors: a non-scalar value, or a non-integer id, must not
    # reach the WHERE clause.
    array_value = Base64.urlsafe_encode64(JSON.dump([[1, 2], 100]), padding: false)
    hash_value  = Base64.urlsafe_encode64(JSON.dump([{ "x" => 1 }, 100]), padding: false)
    string_id   = Base64.urlsafe_encode64(JSON.dump([5, "100"]), padding: false)
    assert_nil CP.decode(array_value)
    assert_nil CP.decode(hash_value)
    assert_nil CP.decode(string_id)
  end

  def test_apply_ignores_type_mismatched_cursor
    # A numeric value forged for a timestamp/text sort would raise a PG
    # type error; apply must fall back to the scope (first page) instead.
    fake_scope = Object.new
    assert_same fake_scope, CP.apply(fake_scope, "stale", [999, 42]),
                "numeric value on a timestamp sort must be ignored"
    # The mirror case: a string value on a numeric sort is also ignored.
    assert_same fake_scope, CP.apply(fake_scope, "pending", ["oops", 42]),
                "string value on a numeric sort must be ignored"
  end

  # M8: a forged non-timestamp string on a timestamp sort would bind into a
  # timestamp column and raise `invalid input syntax for type timestamp`
  # (500). It must fall back to the first page.
  def test_apply_rejects_unparseable_timestamp_cursor
    fake_scope = Object.new
    assert_same fake_scope, CP.apply(fake_scope, "stale", ["zzz", 42]),
                "an unparseable timestamp cursor must fall back to the first page, not 500"
    assert_same fake_scope, CP.apply(fake_scope, "recent", ["not-a-date", 7])
  end

  def test_apply_accepts_valid_iso8601_timestamp_cursor
    applied = Object.new
    fake_scope = Object.new
    fake_scope.define_singleton_method(:where) { |*_args| applied }
    result = CP.apply(fake_scope, "stale", ["2026-01-15T10:30:45.000000Z", 42])
    assert_same applied, result, "a valid ISO8601 timestamp cursor must build a WHERE clause"
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
