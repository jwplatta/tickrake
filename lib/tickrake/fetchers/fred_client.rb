# frozen_string_literal: true

require "net/http"
require "uri"
require "json"

module Tickrake
  module Fetchers
    class FredClient
      BASE = "https://api.stlouisfed.org/fred"

      def initialize
        @api_key = ENV.fetch("FRED_API_KEY") { raise Tickrake::ConfigError, "Missing FRED_API_KEY" }
      end

      def get(path, params = {})
        uri = URI("#{BASE}#{path}")
        uri.query = URI.encode_www_form(params.merge(api_key: @api_key, file_type: "json"))
        response = Net::HTTP.get_response(uri)
        raise Tickrake::Error, "FRED API error #{response.code}: #{response.body[0, 200]}" unless response.is_a?(Net::HTTPSuccess)

        JSON.parse(response.body)
      end
    end
  end
end
