# frozen_string_literal: true

require "csv"

module Tickrake
  module Maintenance
    module Candles
      class Compactor
        HEADERS = %w[datetime open high low close volume].freeze

        def initialize(context:, writer: Tickrake::Storage::CandleParquetWriter.new)
          @context = context
          @writer = writer
        end

        def run
          csv_path = @context.candle_csv_path
          csv_rows = read_csv(csv_path)

          if csv_rows.empty?
            return CompactResult.new(
              success: true,
              provider_name: @context.provider_name,
              symbol: @context.symbol,
              frequency: @context.frequency,
              row_count: 0,
              years_written: [],
              errors: []
            )
          end

          by_year = csv_rows.group_by { |row| Time.iso8601(row[0]).year }
          years_written = []
          total_rows = 0

          by_year.each do |year, new_rows|
            parquet_path = @context.compacted_path(year)
            existing_rows = @writer.read(parquet_path)
            merged = merge_rows(existing_rows, new_rows)
            @writer.write(parquet_path, rows: merged)
            years_written << year
            total_rows += merged.size
          end

          truncate_csv(csv_path)

          @context.logger.info(
            "candle_compactor: compacted #{@context.symbol} #{@context.frequency} " \
            "#{csv_rows.size} row(s) into #{years_written.size} year file(s)"
          )

          CompactResult.new(
            success: true,
            provider_name: @context.provider_name,
            symbol: @context.symbol,
            frequency: @context.frequency,
            row_count: total_rows,
            years_written: years_written,
            errors: []
          )
        rescue StandardError => e
          CompactResult.new(
            success: false,
            provider_name: @context.provider_name,
            symbol: @context.symbol,
            frequency: @context.frequency,
            row_count: 0,
            years_written: [],
            errors: ["#{e.class}: #{e.message}"]
          )
        end

        private

        def read_csv(path)
          return [] unless File.exist?(path)

          rows = []
          CSV.foreach(path, headers: true) do |row|
            next if row["datetime"].nil? || row["datetime"].empty?

            rows << [
              row["datetime"],
              row["open"]&.to_f,
              row["high"]&.to_f,
              row["low"]&.to_f,
              row["close"]&.to_f,
              row["volume"]&.to_i
            ]
          end
          rows
        end

        def merge_rows(existing, new_rows)
          by_datetime = {}
          existing.each { |r| by_datetime[r[0]] = r }
          new_rows.each { |r| by_datetime[r[0]] = r }
          by_datetime.values.sort_by { |r| r[0] }
        end

        def truncate_csv(path)
          File.write(path, HEADERS.join(",") + "\n")
        end
      end
    end
  end
end
