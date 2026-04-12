# frozen_string_literal: true

module Tickrake
  module Providers
    class Schwab < Base
      INDEX_API_SYMBOLS = %w[COMPX DJX MID NDX OEX RUT SPX VIX VIX9D VIX1D XSP].freeze
      FREQUENCY_ALIASES = {
        "1min" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MINUTE,
          frequency: SchwabRb::PriceHistory::Frequencies::EVERY_MINUTE,
          period_type: SchwabRb::PriceHistory::PeriodTypes::DAY,
          period: SchwabRb::PriceHistory::Periods::ONE_DAY
        },
        "5min" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MINUTE,
          frequency: SchwabRb::PriceHistory::Frequencies::EVERY_FIVE_MINUTES,
          period_type: SchwabRb::PriceHistory::PeriodTypes::DAY,
          period: SchwabRb::PriceHistory::Periods::ONE_DAY
        },
        "10min" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MINUTE,
          frequency: SchwabRb::PriceHistory::Frequencies::EVERY_TEN_MINUTES,
          period_type: SchwabRb::PriceHistory::PeriodTypes::DAY,
          period: SchwabRb::PriceHistory::Periods::ONE_DAY
        },
        "15min" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MINUTE,
          frequency: SchwabRb::PriceHistory::Frequencies::EVERY_FIFTEEN_MINUTES,
          period_type: SchwabRb::PriceHistory::PeriodTypes::DAY,
          period: SchwabRb::PriceHistory::Periods::ONE_DAY
        },
        "30min" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MINUTE,
          frequency: SchwabRb::PriceHistory::Frequencies::EVERY_THIRTY_MINUTES,
          period_type: SchwabRb::PriceHistory::PeriodTypes::DAY,
          period: SchwabRb::PriceHistory::Periods::ONE_DAY
        },
        "day" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::DAILY,
          frequency: SchwabRb::PriceHistory::Frequencies::DAILY,
          period_type: SchwabRb::PriceHistory::PeriodTypes::YEAR,
          period: SchwabRb::PriceHistory::Periods::TWENTY_YEARS
        },
        "week" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::WEEKLY,
          frequency: SchwabRb::PriceHistory::Frequencies::WEEKLY,
          period_type: SchwabRb::PriceHistory::PeriodTypes::YEAR,
          period: SchwabRb::PriceHistory::Periods::TWENTY_YEARS
        },
        "month" => {
          frequency_type: SchwabRb::PriceHistory::FrequencyTypes::MONTHLY,
          frequency: SchwabRb::PriceHistory::Frequencies::MONTHLY,
          period_type: SchwabRb::PriceHistory::PeriodTypes::YEAR,
          period: SchwabRb::PriceHistory::Periods::TWENTY_YEARS
        }
      }.freeze

      def initialize(provider_name:, client:)
        super(provider_name: provider_name, adapter_name: "schwab")
        @client = client
      end

      def fetch_bars(symbol:, frequency:, start_date:, end_date:, extended_hours:, previous_close:)
        config = FREQUENCY_ALIASES.fetch(frequency)
        response = @client.get_price_history(
          api_symbol(symbol),
          period_type: config.fetch(:period_type),
          period: config.fetch(:period),
          frequency_type: config.fetch(:frequency_type),
          frequency: config.fetch(:frequency),
          start_datetime: start_date,
          end_datetime: end_date,
          need_extended_hours_data: extended_hours,
          need_previous_close: previous_close,
          return_data_objects: false
        )

        Array(response[:candles]).map do |candle|
          Data::Bar.new(
            datetime: Time.at(candle.fetch(:datetime) / 1000.0).utc,
            open: candle[:open],
            high: candle[:high],
            low: candle[:low],
            close: candle[:close],
            volume: candle[:volume],
            source: provider_name,
            symbol: symbol,
            frequency: frequency
          )
        end
      end

      private

      def api_symbol(symbol)
        raw_symbol = symbol.to_s.strip
        return raw_symbol if raw_symbol.start_with?("$", "/")
        return "$#{raw_symbol}" if INDEX_API_SYMBOLS.include?(raw_symbol.upcase)

        raw_symbol
      end
    end
  end
end
