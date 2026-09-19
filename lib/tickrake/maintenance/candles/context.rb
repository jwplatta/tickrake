# frozen_string_literal: true

module Tickrake
  module Maintenance
    module Candles
      class Context
        attr_reader :config, :provider_name, :frequency, :symbol, :logger, :storage_paths

        def initialize(config:, provider_name:, frequency:, symbol:, logger:, storage_paths: nil)
          @config = config
          @provider_name = provider_name
          @frequency = frequency
          @symbol = symbol
          @logger = logger
          @storage_paths = storage_paths || Tickrake::Storage::Paths.new(config)
        end

        def candle_csv_path
          @storage_paths.candle_path(provider: @provider_name, symbol: @symbol, frequency: @frequency)
        end

        def compacted_path(year)
          @storage_paths.candle_compacted_path(
            provider: @provider_name, symbol: @symbol, frequency: @frequency, year: year
          )
        end
      end
    end
  end
end
