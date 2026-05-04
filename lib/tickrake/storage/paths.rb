# frozen_string_literal: true

require "tzinfo"

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
        filename_timestamp = option_snapshot_filename_time(timestamp)
        File.join(
          provider_options_dir(provider),
          filename_timestamp.strftime("%Y"),
          filename_timestamp.strftime("%m"),
          filename_timestamp.strftime("%d"),
          [
            sanitize_symbol(selected_root),
            "exp#{expiration_date.iso8601}",
            filename_timestamp.strftime("%Y-%m-%d_%H-%M-%S")
          ].join("_") + ".csv"
        )
      end

      private

      def option_snapshot_filename_time(timestamp)
        timezone_name = @config.option_snapshot_filename_timezone.to_s
        return timestamp.utc if timezone_name.empty? || timezone_name.casecmp("utc").zero?

        TZInfo::Timezone.get(timezone_name).utc_to_local(timestamp.utc)
      end

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
