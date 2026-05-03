# frozen_string_literal: true

require "digest/sha1"
require "pathname"

module DispatchPolicy
  # Vendored static assets served by AssetsController. Bodies are read
  # once at boot and the digest is embedded in the URL so the response
  # can be marked `Cache-Control: immutable` — bumping the vendored file
  # produces a new digest and the host's browsers refetch automatically.
  #
  # To upgrade Turbo (current: 8.0.4), overwrite the file from the same
  # CDN/version pair the rest of the Hotwire ecosystem uses:
  #
  #   curl -fsSL https://cdn.jsdelivr.net/npm/@hotwired/turbo@<VERSION>/dist/turbo.es2017-umd.min.js \
  #     -o app/assets/javascripts/dispatch_policy/turbo.es2017-umd.min.js
  #
  # No other code change is required — TURBO_DIGEST is content-addressed.
  module Assets
    JS_ROOT    = Pathname.new(File.expand_path("../../app/assets/javascripts/dispatch_policy", __dir__))
    IMAGE_ROOT = Pathname.new(File.expand_path("../../app/assets/images/dispatch_policy", __dir__))

    TURBO_BODY   = JS_ROOT.join("turbo.es2017-umd.min.js").read.freeze
    TURBO_DIGEST = Digest::SHA1.hexdigest(TURBO_BODY)[0, 12].freeze

    LOGO_BODY   = IMAGE_ROOT.join("logo.svg").read.freeze
    LOGO_DIGEST = Digest::SHA1.hexdigest(LOGO_BODY)[0, 12].freeze
  end
end
