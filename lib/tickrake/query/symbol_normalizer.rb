# frozen_string_literal: true

module Tickrake
  module Query
    class SymbolNormalizer
      def canonical(symbol)
        value = symbol.to_s.strip.upcase
        value.sub(/\A\$/, "")
      end

      def storage_token(symbol)
        cleaned = canonical(symbol).gsub(/[^A-Z0-9]+/, "_").gsub(/\A_+|_+\z/, "")
        cleaned.empty? ? "SYMBOL" : cleaned
      end

      def same_symbol?(left, right)
        canonical(left) == canonical(right)
      end
    end
  end
end
