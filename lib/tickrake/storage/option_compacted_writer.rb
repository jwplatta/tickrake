# frozen_string_literal: true

module Tickrake
  module Storage
    class OptionCompactedWriter
      Result = Struct.new(:csv_path, :parquet_path, keyword_init: true)

      def initialize(csv_writer: Tickrake::Storage::CsvWriter.new, parquet_writer: Tickrake::Storage::ParquetWriter.new)
        @csv_writer = csv_writer
        @parquet_writer = parquet_writer
      end

      def write(csv_path:, parquet_path:, headers:, rows:)
        @csv_writer.write(csv_path, headers: headers, rows: rows)
        @parquet_writer.write(parquet_path, headers: headers, rows: rows)
        Result.new(csv_path: csv_path, parquet_path: parquet_path)
      end
    end
  end
end
