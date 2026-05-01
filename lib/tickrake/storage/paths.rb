# frozen_string_literal: true

module Tickrake
  module Storage
    class Paths
      def initialize(config)
        @config = config
        @symbol_normalizer = Tickrake::Query::SymbolNormalizer.new
      end

      def candle_path(provider:, symbol:, frequency:)
        File.join(provider_history_dir(provider), "#{sanitize_symbol(symbol)}_#{frequency}.csv")
      end

      def option_sample_path(provider:, symbol:, expiration_date:, timestamp:, root: nil)
        selected_root = root || symbol
        File.join(
          provider_options_dir(provider),
          timestamp.utc.strftime("%Y"),
          timestamp.utc.strftime("%m"),
          timestamp.utc.strftime("%d"),
          [
            sanitize_symbol(selected_root),
            "exp#{expiration_date.iso8601}",
            timestamp.utc.strftime("%Y-%m-%d_%H-%M-%S")
          ].join("_") + ".csv"
        )
      end

      private

      def provider_history_dir(provider)
        File.join(@config.history_dir, provider.to_s)
      end

      def provider_options_dir(provider)
        File.join(@config.options_dir, provider.to_s)
      end

      def sanitize_symbol(symbol)
        @symbol_normalizer.storage_token(symbol)
      end
    end
  end
end
