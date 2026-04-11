# frozen_string_literal: true

module Tickrake
  module Storage
    class Paths
      def initialize(config)
        @config = config
      end

      def candle_path(provider:, symbol:, frequency:)
        File.join(provider_history_dir(provider), "#{sanitize_symbol(symbol)}_#{frequency}.csv")
      end

      def option_sample_path(provider:, symbol:, expiration_date:, timestamp:, root: nil)
        selected_root = root || symbol
        File.join(
          provider_options_dir(provider),
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
        sanitized_symbol = symbol.to_s.gsub(/[^a-zA-Z0-9]+/, "_").gsub(/\A_+|_+\z/, "")
        sanitized_symbol.empty? ? "symbol" : sanitized_symbol
      end
    end
  end
end
