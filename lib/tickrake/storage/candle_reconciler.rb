# frozen_string_literal: true

module Tickrake
  module Storage
    class CandleReconciler
      HEADERS = %w[datetime open high low close volume].freeze

      def initialize(csv_writer: CsvWriter.new)
        @csv_writer = csv_writer
      end

      def write(path:, bars:)
        merged_bars = merge(existing_bars(path), bars)
        @csv_writer.write(
          path,
          headers: HEADERS,
          rows: merged_bars.map do |bar|
            [
              bar.utc_datetime.iso8601,
              bar.open,
              bar.high,
              bar.low,
              bar.close,
              bar.volume
            ]
          end
        )
      end

      private

      def existing_bars(path)
        return [] unless File.exist?(path)

        CSV.read(path, headers: true).map do |row|
          Data::Bar.new(
            datetime: Time.iso8601(row.fetch("datetime")).utc,
            open: row.fetch("open").to_f,
            high: row.fetch("high").to_f,
            low: row.fetch("low").to_f,
            close: row.fetch("close").to_f,
            volume: row.fetch("volume").to_i
          )
        end
      end

      def merge(left, right)
        (Array(left) + Array(right)).each_with_object({}) do |bar, by_timestamp|
          by_timestamp[bar.utc_datetime.iso8601] = bar
        end.values.sort_by(&:utc_datetime)
      end
    end
  end
end
