# frozen_string_literal: true

module Tickrake
  module Storage
    class ParquetWriter
      def write(path, headers:, rows:)
        require "parquet"

        directory = File.dirname(path)
        FileUtils.mkdir_p(directory)
        tmp_path = "#{path}.tmp"

        schema = headers.map { |header| { header => "string" } }
        Parquet.write_rows(rows.each, schema: schema, write_to: tmp_path)
        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && File.exist?(tmp_path)
      end
    end
  end
end
