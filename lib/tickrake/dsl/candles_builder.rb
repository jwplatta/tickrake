# frozen_string_literal: true

module Tickrake
  module DSL
    class CandlesBuilder
      FREQUENCY_ALIASES = {
        "minute" => "1min", "1m" => "1min", "1min" => "1min",
        "5m" => "5min",  "5min" => "5min",
        "10m" => "10min", "10min" => "10min",
        "15m" => "15min", "15min" => "15min",
        "30m" => "30min", "30min" => "30min",
        "day" => "day",   "daily" => "day",
        "week" => "week", "weekly" => "week",
        "month" => "month", "monthly" => "month"
      }.freeze

      def initialize
        @frequencies = []
        @start_date = nil
      end

      def frequency(value)
        @frequencies = [normalize_frequency(value)]
      end

      def frequencies(*args)
        list = args.flatten
        @frequencies = list.map { |v| normalize_frequency(v) }.uniq
      end

      def start_date(value)
        @start_date = Date.iso8601(value.to_s)
      end

      def build!
        { frequencies: @frequencies, start_date: @start_date }
      end

      private

      def normalize_frequency(value)
        normalized = value.to_s.downcase.strip
        FREQUENCY_ALIASES.fetch(normalized) do
          raise Tickrake::Error, "Unsupported candle frequency: #{value.inspect}"
        end
      end
    end
  end
end
