# frozen_string_literal: true

module Tickrake
  module Core
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
        "hour" => "1hour",
        "1h" => "1hour",
        "1hour" => "1hour",
        "day" => "day",
        "daily" => "day",
        "1d" => "day",
        "week" => "week",
        "weekly" => "week",
        "1w" => "week",
        "month" => "month",
        "monthly" => "month"
      }.freeze

      def normalize(frequency)
        normalized = ALIASES[frequency.to_s.strip.downcase]
        raise Tickrake::Error, "Unknown candle frequency: `#{frequency}`." unless normalized

        normalized
      end
    end
  end
  # Backward alias in case anything expects Tickrake::FrequencyNormalizer
  FrequencyNormalizer = Core::FrequencyNormalizer
end
