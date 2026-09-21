# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class EconomicEventsParquetWriter
      SCHEMA = [
        { "event_datetime"  => "string" },
        { "event_name"      => "string" },
        { "category"        => "string" },
        { "source"          => "string" },
        { "actual"          => "double" },
        { "estimate"        => "double" },
        { "previous"        => "double" },
        { "unit"            => "string" },
        { "symbol"          => "string" },
        { "company_name"    => "string" },
        { "time_of_day"     => "string" },
        { "release_id"      => "int64"  },
        { "series_id"       => "string" },
        { "fetched_at"      => "string" }
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
        File.delete(tmp_path) if defined?(tmp_path) && tmp_path && File.exist?(tmp_path)
      end

      private

      def typed_rows(rows)
        rows.map do |row|
          [
            row[:event_datetime]&.to_s,
            row[:event_name]&.to_s,
            row[:category]&.to_s,
            row[:source]&.to_s,
            row[:actual]&.to_f,
            row[:estimate]&.to_f,
            row[:previous]&.to_f,
            row[:unit]&.to_s,
            row[:symbol]&.to_s,
            row[:company_name]&.to_s,
            row[:time_of_day]&.to_s,
            row[:release_id]&.to_i,
            row[:series_id]&.to_s,
            row[:fetched_at]&.to_s
          ]
        end
      end
    end
  end
end
