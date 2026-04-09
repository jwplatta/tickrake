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

    module_function :filter_map
  end
end
