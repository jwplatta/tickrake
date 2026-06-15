# frozen_string_literal: true

module Tickrake
  module Query
    class CandleMetadata
      def initialize(config:, symbol_normalizer: SymbolNormalizer.new)
        @config = config
        @symbol_normalizer = symbol_normalizer
      end

      def build(path:, provider_name:)
        basename = File.basename(path, ".csv")
        match = /\A(?<ticker>.+)_(?<frequency>[^_]+)\z/.match(basename)
        return nil unless match

        stat = File.stat(path)
        row_count = 0
        first_observed_at = nil
        last_observed_at = nil

        File.foreach(path).with_index do |line, index|
          next if index.zero?
          next if line.strip.empty?

          row_count += 1
          observed_at = line.split(",", 2).first
          first_observed_at ||= observed_at
          last_observed_at = observed_at
        end

        {
          path: path,
          dataset_type: "candles",
          provider_name: provider_name,
          ticker: canonical_ticker_for(match[:ticker], provider_name: provider_name),
          frequency: match[:frequency],
          row_count: row_count,
          first_observed_at: first_observed_at,
          last_observed_at: last_observed_at,
          file_mtime: stat.mtime.to_i,
          file_size: stat.size,
          updated_at: Time.now
        }
      end

      private

      def canonical_ticker_for(path_token, provider_name:)
        provider_definition = @config.provider_definition(provider_name)
        mapped_symbol = provider_definition.symbol_map.fetch(path_token, nil)
        @symbol_normalizer.canonical(mapped_symbol || path_token)
      end
    end
  end
end
