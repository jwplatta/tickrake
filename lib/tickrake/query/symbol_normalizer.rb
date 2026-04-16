# frozen_string_literal: true

module Tickrake
  module Query
    class SymbolNormalizer
      def canonical(symbol, provider_definition: nil)
        value = mapped_symbol(symbol, provider_definition: provider_definition)
        return value if value.start_with?("^")

        value.sub(/\A\$/, "")
      end

      def storage_token(symbol, provider_definition: nil)
        cleaned = canonical(symbol, provider_definition: provider_definition).gsub(/[^A-Z0-9^]+/, "_").gsub(/\A_+|_+\z/, "")
        cleaned.empty? ? "SYMBOL" : cleaned
      end

      def same_symbol?(left, right, provider_definition: nil)
        canonical(left, provider_definition: provider_definition) == canonical(right, provider_definition: provider_definition)
      end

      private

      def mapped_symbol(symbol, provider_definition:)
        raw = symbol.to_s.strip
        symbol_map = provider_definition&.symbol_map || {}
        mapped = symbol_map.fetch(raw, nil) || symbol_map.fetch(raw.upcase, nil)
        (mapped || raw).strip.upcase
      end
    end
  end
end
