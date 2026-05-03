# frozen_string_literal: true

module DispatchPolicy
  # Serves vendored static assets at content-addressed URLs so browsers
  # can cache them forever. The digest is part of the URL, not a query
  # string, so caches keyed on path alone still bust on upgrade.
  class AssetsController < ApplicationController
    skip_forgery_protection

    def turbo
      serve(Assets::TURBO_BODY, Assets::TURBO_DIGEST, "application/javascript")
    end

    def logo
      serve(Assets::LOGO_BODY, Assets::LOGO_DIGEST, "image/svg+xml")
    end

    private

    def serve(body, digest, content_type)
      if params[:digest] != digest
        head :not_found
        return
      end

      response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
      response.headers["ETag"]          = %("#{digest}")
      send_data body, type: content_type, disposition: "inline"
    end
  end
end
