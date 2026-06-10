# frozen_string_literal: true

require_relative "../test_helper"

class ContextTest < Minitest::Test
  Context = DispatchPolicy::Context

  def test_top_level_indifferent_access
    ctx = Context.wrap(rate: 10, "endpoint" => "ep1")
    assert_equal 10,    ctx[:rate]
    assert_equal 10,    ctx["rate"]
    assert_equal "ep1", ctx[:endpoint]
  end

  # The bug: nested hashes were stored string-keyed, so a symbol lookup on
  # the nested hash returned nil and a gate proc reading ctx[:limits][:max]
  # would silently get nil.
  def test_nested_indifferent_access
    ctx = Context.wrap(limits: { max: 5, "per" => 60 })
    assert_equal 5,  ctx[:limits][:max]
    assert_equal 5,  ctx[:limits]["max"]
    assert_equal 60, ctx[:limits][:per]
    assert_equal 60, ctx.fetch(:limits)[:per]
  end

  def test_to_jsonb_stays_plain_string_keyed
    ctx = Context.wrap(limits: { max: 5 })
    assert_equal({ "limits" => { "max" => 5 } }, ctx.to_jsonb)
    # to_jsonb must be a plain Hash (string keys) for storage, not an HWIA.
    assert_instance_of Hash, ctx.to_jsonb
  end

  def test_scalars_pass_through_untouched
    ctx = Context.wrap(rate: 10, name: "x", flag: true)
    assert_equal 10,  ctx[:rate]
    assert_equal "x", ctx[:name]
    assert_equal true, ctx[:flag]
  end
end
