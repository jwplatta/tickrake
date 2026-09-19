# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class CandleParquetWriter
      SCHEMA = [
        { "datetime" => "string" },
        { "open"     => "double" },
        { "high"     => "double" },
        { "low"      => "double" },
        { "close"    => "double" },
        { "volume"   => "int64" }
      ].freeze

      def write(path, rows:)
        FileUtils.mkdir_p(File.dirname(path))
        tmp_path = "#{path}.tmp"

        Parquet.write_rows(
          rows.map { |r| [r[0].to_s, r[1]&.to_f, r[2]&.to_f, r[3]&.to_f, r[4]&.to_f, r[5]&.to_i] },
          schema: SCHEMA,
          write_to: tmp_path
        )
        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && tmp_path && File.exist?(tmp_path)
      end

      def read(path)
        return [] unless File.exist?(path)

        rows = []
        Parquet.each_row(path) do |row|
          rows << [row["datetime"], row["open"], row["high"], row["low"], row["close"], row["volume"]]
        end
        rows
      end
    end
  end
end
