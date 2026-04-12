# frozen_string_literal: true

module Tickrake
  module Query
    class FrequencyNormalizer
      ALIASES = {
        "minute" => "1min",
        "1m" => "1min",
        "1min" => "1min",
        "5m" => "5min",
        "5min" => "5min",
        "10m" => "10min",
        "10min" => "10min",
        "15m" => "15min",
        "15min" => "15min",
        "30m" => "30min",
        "30min" => "30min",
        "day" => "day",
        "daily" => "day",
        "week" => "week",
        "weekly" => "week",
        "month" => "month",
        "monthly" => "month"
      }.freeze

      def normalize(value)
        normalized = value.to_s.downcase.strip
        return nil if normalized.empty?
        return ALIASES.fetch(normalized) if ALIASES.key?(normalized)

        raise Tickrake::Error, "Unsupported candle frequency: #{value}"
      end
    end
  end
end
