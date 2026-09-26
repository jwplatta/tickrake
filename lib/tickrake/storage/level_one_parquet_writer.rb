# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class LevelOneParquetWriter
      # Schema for Level One event Parquet files.
      #
      # Timestamp semantics and caveats:
      # - received_at_ms: Local collector receipt time (ms since epoch UTC). Safest cutoff for
      #   point-in-time analysis ("what data did the collector have by time T?").
      # - quote_time_ms: Upstream feed timestamp attached to quote info (bid/ask).
      #   Note: Do not assume received_at_ms - quote_time_ms is a precise network latency measurement;
      #   independent clocks or feed stages can cause small timing reversals (e.g. quote time appearing
      #   tens of ms after receive time).
      # - trade_time_ms: Upstream feed timestamp attached to last-trade info. Nil/NaT means trade info
      #   was absent in that specific message, not that the symbol never traded. Conversely, do not treat
      #   every row with trade_time_ms as a new trade without verifying whether trade fields updated.
      SCHEMA = [
        { "received_at_ms" => "int64" }, # Collector receipt time (ms since epoch UTC)
        { "symbol"         => "string" },
        { "service"        => "string" },
        { "quote_time_ms"  => "int64" },  # Feed quote timestamp (ms since epoch UTC)
        { "trade_time_ms"  => "int64" },  # Feed trade timestamp (ms since epoch UTC, nullable)
        { "bid"            => "double" },
        { "ask"            => "double" },
        { "last"           => "double" },
        { "bid_size"       => "int64" },
        { "ask_size"       => "int64" },
        { "volume"         => "int64" },
        { "mark"           => "double" },
        { "extra_json"     => "string" }
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
            row["quote_time_ms"]&.to_i,
            row["trade_time_ms"]&.to_i,
            row["bid"]&.to_f,
            row["ask"]&.to_f,
            row["last"]&.to_f,
            row["bid_size"]&.to_i,
            row["ask_size"]&.to_i,
            row["volume"]&.to_i,
            row["mark"]&.to_f,
            row["extra_json"]
          ]
        end
      end
    end
  end
end
