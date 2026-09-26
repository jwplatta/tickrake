# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class OrderBookParquetWriter
      # Schema for Order Book (Level 2 depth) Parquet files.
      #
      # Timestamps:
      # - received_at_ms: Local collector receipt time (ms since epoch UTC). Safest cutoff for
      #   point-in-time analysis.
      # - book_time_ms: Upstream market snapshot timestamp (ms since epoch UTC) from the broker/exchange.
      SCHEMA = [
        { "received_at_ms" => "int64" }, # Collector receipt time (ms since epoch UTC)
        { "symbol"         => "string" },
        { "service"        => "string" },
        { "book_time_ms"   => "int64" },  # Market snapshot timestamp (ms since epoch UTC)
        { "bids_json"      => "string" },
        { "asks_json"      => "string" }
      ].freeze

      def write(path, rows:)
        FileUtils.mkdir_p(File.dirname(path))
        tmp_path = "#{path}.tmp"

        Parquet.write_rows(
          typed_rows(rows),
          schema: SCHEMA,
          write_to: tmp_path
        )
        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && File.exist?(tmp_path)
      end

      private

      def typed_rows(rows)
        rows.map do |row|
          [
            row.fetch("received_at").to_i,
            row.fetch("symbol").to_s,
            row.fetch("service").to_s,
            row["book_time_ms"]&.to_i,
            row["bids_json"],
            row["asks_json"]
          ]
        end
      end
    end
  end
end
