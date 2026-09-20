# frozen_string_literal: true

require "net/http"
require "uri"
require "csv"
require "date"

module Tickrake
  module Fetchers
    class AlphaVantageEarnings
      BASE_URL = "https://www.alphavantage.co/query"

      def initialize
        @api_key = ENV.fetch("ALPHA_VANTAGE_API_KEY") { raise Tickrake::ConfigError, "Missing ALPHA_VANTAGE_API_KEY" }
      end

      def fetch(from_date:, to_date:, horizon: "3month")
        fetched_at = Time.now.utc.iso8601
        csv_body = download_csv(horizon)
        parse_csv(csv_body, from_date, to_date, fetched_at)
      end

      private

      def download_csv(horizon)
        uri = URI(BASE_URL)
        uri.query = URI.encode_www_form(
          function: "EARNINGS_CALENDAR",
          horizon: horizon,
          apikey: @api_key
        )
        response = Net::HTTP.get_response(uri)
        raise Tickrake::Error, "Alpha Vantage error #{response.code}: #{response.body[0, 200]}" unless response.is_a?(Net::HTTPSuccess)

        response.body
      end

      def parse_csv(body, from_date, to_date, fetched_at)
        rows = []
        CSV.parse(body, headers: true) do |row|
          report_date_str = row["reportDate"]
          next unless report_date_str

          date = begin
            Date.parse(report_date_str)
          rescue ArgumentError
            next
          end
          next unless date >= from_date && date <= to_date

          estimate_str = row["estimate"]
          estimate = estimate_str && !estimate_str.strip.empty? ? estimate_str.to_f : nil

          rows << {
            event_datetime: "#{date.iso8601}T00:00:00Z",
            event_name: "Earnings",
            category: "earnings",
            source: "alpha_vantage",
            actual: nil,
            estimate: estimate,
            previous: nil,
            unit: "USD",
            symbol: row["symbol"]&.strip,
            company_name: row["name"]&.strip,
            time_of_day: row["reportTime"]&.strip,
            release_id: nil,
            series_id: nil,
            fetched_at: fetched_at
          }
        end
        rows
      rescue CSV::MalformedCSVError => e
        raise Tickrake::Error, "Alpha Vantage CSV parse error: #{e.message}"
      end
    end
  end
end
