# frozen_string_literal: true

module DispatchPolicy
  # Serves vendored JS as a static file with content-addressed URLs so
  # browsers can cache it forever. The digest is part of the URL, not a
  # query string, so caches keyed on path alone still bust on upgrade.
  class AssetsController < ApplicationController
    skip_forgery_protection

    def turbo
      serve(Assets::TURBO_BODY, Assets::TURBO_DIGEST)
    end

    private

    def serve(body, digest)
      if params[:digest] != digest
        head :not_found
        return
      end

      response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
      response.headers["ETag"]          = %("#{digest}")
      send_data body, type: "application/javascript", disposition: "inline"
    end
  end
end
