# frozen_string_literal: true

module Tickrake
  module IndexData
    class SymbolNormalizer
      def normalize(symbol)
        symbol.to_s.strip.upcase.tr(".", "-")
      end
    end
  end
end
