# frozen_string_literal: true

module Tickrake
  module PathSupport
    module_function

    def expand_path(path)
      File.expand_path(path.to_s)
    end

    def sanitize_symbol(symbol)
      cleaned = symbol.to_s.gsub(/[^a-zA-Z0-9]+/, "_").gsub(/\A_+|_+\z/, "")
      cleaned.empty? ? "symbol" : cleaned
    end
  end
end
