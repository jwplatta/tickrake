# frozen_string_literal: true

module Tickrake
  module Serializers
    module_function

    OPTION_HEADERS = %w[
      contract_type symbol description strike expiration_date mark bid bid_size ask ask_size
      last last_size open_interest total_volume delta gamma theta vega rho volatility
      theoretical_volatility theoretical_option_value intrinsic_value extrinsic_value
      underlying_price
    ].freeze

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

    def price_history_request(frequency)
      FREQUENCY_ALIASES.fetch(frequency)
    end

    def history_path(directory:, symbol:, frequency:)
      File.join(directory, "#{Tickrake::PathSupport.sanitize_symbol(symbol)}_#{frequency}.csv")
    end

    def option_path(directory:, root:, expiration_date:, sampled_at:)
      File.join(
        directory,
        [
          Tickrake::PathSupport.sanitize_symbol(root),
          "exp#{expiration_date.iso8601}",
          sampled_at.strftime("%Y-%m-%d_%H-%M-%S")
        ].join("_") + ".csv"
      )
    end

    def write_history_csv(path, response)
      normalized = normalize_price_history(response)
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, CSV.generate do |csv|
        csv << %w[datetime open high low close volume]
        normalized.each do |candle|
          csv << candle
        end
      end)
      path
    end

    def merge_history(existing_path, response)
      new_rows = normalize_price_history(response)
      existing_rows =
        if File.exist?(existing_path)
          CSV.read(existing_path, headers: true).map do |row|
            [row["datetime"], row["open"], row["high"], row["low"], row["close"], row["volume"]]
          end
        else
          []
        end

      merged = (existing_rows + new_rows).each_with_object({}) { |row, by_dt| by_dt[row[0]] = row }.values.sort_by(&:first)
      FileUtils.mkdir_p(File.dirname(existing_path))
      File.write(existing_path, CSV.generate do |csv|
        csv << %w[datetime open high low close volume]
        merged.each { |row| csv << row }
      end)
      existing_path
    end

    def write_option_csv(path, response)
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, CSV.generate do |csv|
        csv << OPTION_HEADERS
        option_rows(response).each do |option|
          csv << [
            option[:putCall],
            option[:symbol],
            option[:description],
            option[:strikePrice],
            normalize_option_date(option[:expirationDate]),
            option[:mark],
            option[:bid],
            option[:bidSize],
            option[:ask],
            option[:askSize],
            option[:last],
            option[:lastSize],
            option[:openInterest],
            option[:totalVolume],
            option[:delta],
            option[:gamma],
            option[:theta],
            option[:vega],
            option[:rho],
            option[:volatility],
            option[:theoreticalVolatility],
            option[:theoreticalOptionValue],
            option[:intrinsicValue],
            option[:extrinsicValue],
            response[:underlyingPrice]
          ]
        end
      end)
      path
    end

    def filter_option_chain(response, root)
      return response if root.nil? || root.empty?

      normalized_root = root.upcase
      {
        **response,
        callExpDateMap: filter_map(response[:callExpDateMap], normalized_root),
        putExpDateMap: filter_map(response[:putExpDateMap], normalized_root)
      }
    end

    def option_rows(response)
      rows = [response[:callExpDateMap], response[:putExpDateMap]].compact.flat_map do |date_map|
        date_map.values.flat_map do |strikes|
          strikes.values.flatten.map { |option| option.transform_keys(&:to_sym) }
        end
      end

      rows.sort_by { |option| [normalize_option_date(option[:expirationDate]).to_s, option[:putCall].to_s, option[:strikePrice].to_f] }
    end

    def normalize_option_date(value)
      return if value.nil?

      Date.parse(value.to_s).iso8601
    end

    def normalize_price_history(response)
      Array(response[:candles]).map do |candle|
        normalized = candle.transform_keys(&:to_sym)
        [
          Time.at(normalized.fetch(:datetime) / 1000.0).utc.iso8601,
          normalized[:open],
          normalized[:high],
          normalized[:low],
          normalized[:close],
          normalized[:volume]
        ]
      end.sort_by(&:first)
    end

    def filter_map(date_map, root)
      return {} unless date_map

      date_map.each_with_object({}) do |(expiration_key, strikes), filtered_dates|
        filtered_strikes = strikes.each_with_object({}) do |(strike, contracts), filtered_by_strike|
          matching = contracts.select { |contract| contract[:optionRoot].to_s.upcase == root }
          filtered_by_strike[strike] = matching if matching.any?
        end
        filtered_dates[expiration_key] = filtered_strikes if filtered_strikes.any?
      end
    end

    module_function :normalize_price_history, :filter_map
  end
end
