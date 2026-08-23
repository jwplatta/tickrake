# frozen_string_literal: true

module Tickrake
  module Index
    class RootIndexBuilder
      SCHEMA_VERSION = 1

      def initialize(tracker:, options_dir:)
        @tracker = tracker
        @options_dir = options_dir
      end

      def build(provider:, root:)
        {
          "schema_version" => SCHEMA_VERSION,
          "provider" => provider,
          "root" => root,
          "updated_at" => Time.now.utc.iso8601,
          "historical" => build_historical(provider, root),
          "intraday" => build_intraday(provider, root)
        }
      end

      private

      def build_historical(provider, root)
        rows = @tracker.historical_index_rows(provider_name: provider, root: root)
        grouped = rows.group_by { |row| row.fetch("sample_date") }

        grouped.map do |sample_date, date_rows|
          files = {}
          date_rows.each do |row|
            format = row.fetch("storage_format")
            next unless format

            uri = UriBuilder.build(
              path: row.fetch("path"),
              storage_location: row.fetch("storage_location"),
              remote_uri: row.fetch("remote_uri")
            )
            files[format] = {
              "uri" => uri,
              "row_count" => row.fetch("row_count"),
              "source_file_count" => row.fetch("source_file_count")
            }.compact
          end

          parquet_row = date_rows.find { |r| r.fetch("storage_format") == "parquet" }
          csv_row = date_rows.find { |r| r.fetch("storage_format") == "csv" }
          representative = parquet_row || csv_row

          {
            "sample_date" => sample_date,
            "status" => "ready",
            "files" => files,
            "first_observed_at" => representative&.fetch("first_observed_at"),
            "last_observed_at" => representative&.fetch("last_observed_at")
          }
        end.sort_by { |entry| entry.fetch("sample_date") }
      end

      def build_intraday(provider, root)
        rows = @tracker.intraday_index_rows(provider_name: provider, root: root)
        return nil if rows.empty?

        first = rows.first
        files = rows.map do |row|
          {
            "expiration_date" => row.fetch("expiration_date"),
            "format" => "csv",
            "uri" => UriBuilder.build(
              path: row.fetch("path"),
              storage_location: "local",
              remote_uri: nil
            ),
            "row_count" => row.fetch("row_count")
          }
        end

        {
          "collection_id" => first.fetch("collection_id"),
          "sample_date" => first.fetch("sample_date"),
          "sampled_at" => first.fetch("sampled_at"),
          "status" => "complete",
          "files" => files
        }
      end
    end
  end
end
