# frozen_string_literal: true

module Tickrake
  module IndexData
    class Importer
      MEMBERSHIP_HEADERS = %w[index_code canonical_ticker start_date end_date].freeze
      TICKER_HEADERS = %w[
        canonical_ticker
        security_name
        gics_sector
        gics_sub_industry
        headquarters_location
        cik
        founded
        status
      ].freeze
      ALIAS_HEADERS = %w[
        canonical_ticker
        alias_ticker
        start_date
        end_date
      ].freeze
      INDEX_NAMES = {
        "SP500" => "S&P 500"
      }.freeze

      def initialize(tracker:)
        @tracker = tracker
      end

      def import!(memberships_path:, tickers_path: nil, alias_history_path: nil)
        memberships = read_csv!(memberships_path, MEMBERSHIP_HEADERS)
        tickers = tickers_path ? read_csv!(tickers_path, TICKER_HEADERS) : []
        alias_history = alias_history_path ? read_csv!(alias_history_path, ALIAS_HEADERS) : []

        @tracker.with_transaction do
          @tracker.upsert_tickers(tickers)
          @tracker.replace_ticker_alias_history(alias_history)

          memberships.group_by { |row| row.fetch("index_code") }.each do |index_code, grouped_rows|
            @tracker.replace_market_index_memberships(
              index_code: index_code,
              index_name: INDEX_NAMES.fetch(index_code, index_code),
              rows: grouped_rows
            )
          end
        end
      end

      private

      def read_csv!(path, expected_headers)
        rows = CSV.read(path, headers: true, encoding: "bom|utf-8")
        headers = rows.headers
        return rows.map(&:to_h) if headers == expected_headers

        raise Tickrake::Error, "Unexpected headers in #{path}. Expected #{expected_headers.join(', ')}."
      rescue Errno::ENOENT
        raise Tickrake::Error, "Missing index data file: #{path}"
      end
    end
  end
end
