# frozen_string_literal: true

module Tickrake
  module Query
    class Engine
      VALID_TYPES = %w[candles options].freeze
      VALID_FORMATS = %w[text json].freeze

      def initialize(config:, tracker:, stdout:)
        @config = config
        @tracker = tracker
        @stdout = stdout
      end

      def run(type: nil, provider_name: nil, ticker: nil, frequency: nil, start_date: nil, end_date: nil, format: "text")
        raise Tickrake::Error, "Provide at least one of --provider or --ticker." unless provider_name || ticker
        raise Tickrake::Error, "Unsupported query type: #{type}" if type && !VALID_TYPES.include?(type)
        raise Tickrake::Error, "Unsupported output format: #{format}" unless VALID_FORMATS.include?(format)
        raise Tickrake::Error, "--frequency can only be used with candle queries." if frequency && type == "options"
        if provider_name
          @config.provider_definition(provider_name)
        end

        filters = {
          type: type,
          provider: provider_name,
          ticker: ticker,
          frequency: frequency,
          start_date: start_date,
          end_date: end_date,
          format: format
        }

        results = []
        if type.nil? || type == "candles"
          results.concat(
            CandlesScanner.new(config: @config, tracker: @tracker).scan(
              provider_name: provider_name,
              ticker: ticker,
              frequency: frequency,
              start_date: start_date,
              end_date: end_date
            )
          )
        end
        if type.nil? || type == "options"
          results.concat(
            OptionsScanner.new(config: @config, tracker: @tracker).scan(
              provider_name: provider_name,
              ticker: ticker,
              start_date: start_date,
              end_date: end_date
            )
          )
        end

        formatter_for(format).format(results: results, filters: filters).tap do |output|
          @stdout.print(output)
        end
      end

      private

      def formatter_for(format)
        case format
        when "json"
          JsonFormatter.new
        else
          TextFormatter.new
        end
      end
    end
  end
end
