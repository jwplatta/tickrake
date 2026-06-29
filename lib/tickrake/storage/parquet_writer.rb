# frozen_string_literal: true

require "parquet"

module Tickrake
  module Storage
    class ParquetWriter
      DOUBLE_COLUMNS = %w[
        strike
        open
        high
        low
        close
        mark
        bid
        ask
        last
        last_size
        open_interest
        total_volume
        delta
        gamma
        theta
        vega
        rho
        volatility
        theoretical_volatility
        theoretical_option_value
        intrinsic_value
        extrinsic_value
        underlying_price
      ].freeze

      INT64_COLUMNS = %w[
        bid_size
        ask_size
        transactions
      ].freeze

      TIMESTAMP_COLUMNS = %w[sampled_at].freeze

      def write(path, headers:, rows:)
        directory = File.dirname(path)
        FileUtils.mkdir_p(directory)
        tmp_path = "#{path}.tmp"

        Parquet.write_rows(
          typed_rows(headers, rows),
          schema: schema_for(headers),
          write_to: tmp_path
        )
        File.rename(tmp_path, path)
        path
      ensure
        File.delete(tmp_path) if defined?(tmp_path) && File.exist?(tmp_path)
      end

      private

      def schema_for(headers)
        headers.map do |header|
          { header => parquet_field_type(header).to_s }
        end
      end

      def typed_rows(headers, rows)
        rows.map do |row|
          headers.each_with_index.map do |header, index|
            coerce_value(header, row[index])
          end
        end
      end

      def parquet_field_type(header)
        return :double if DOUBLE_COLUMNS.include?(header)
        return :int64 if INT64_COLUMNS.include?(header)
        return :timestamp_micros if TIMESTAMP_COLUMNS.include?(header)

        :string
      end

      def coerce_value(header, value)
        return nil if value.nil?

        stripped = value.is_a?(String) ? value.strip : value
        return nil if stripped == ""

        case header
        when *DOUBLE_COLUMNS
          Float(stripped)
        when *INT64_COLUMNS
          integer_value(stripped)
        when *TIMESTAMP_COLUMNS
          time_value(stripped)
        else
          stripped.to_s
        end
      end

      def integer_value(value)
        return value if value.is_a?(Integer)

        Integer(value)
      rescue ArgumentError, TypeError
        Integer(Float(value))
      end

      def time_value(value)
        return value if value.is_a?(Time)

        Time.iso8601(value.to_s).utc
      end
    end
  end
end
