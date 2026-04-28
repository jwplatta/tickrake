# frozen_string_literal: true

module Tickrake
  module Importers
    class MassiveOptionSymbol
      Parsed = Struct.new(:massive_root, :expiration_date, :contract_type, :strike, :symbol, :description, keyword_init: true)

      TICKER_PATTERN = /\AO:(?<massive_root>[A-Z0-9]+)(?<year>\d{2})(?<month>\d{2})(?<day>\d{2})(?<type>[CP])(?<strike>\d{8})\z/.freeze

      def self.parse(ticker)
        new.parse(ticker)
      end

      def parse(ticker)
        raw = ticker.to_s.strip.upcase
        match = TICKER_PATTERN.match(raw)
        raise Tickrake::Error, "Invalid Massive option ticker `#{ticker}`." unless match

        expiration = Date.new(
          2000 + Integer(match[:year], 10),
          Integer(match[:month], 10),
          Integer(match[:day], 10)
        )
        massive_root = match[:massive_root]
        contract_type = match[:type] == "C" ? "CALL" : "PUT"
        strike = Integer(match[:strike], 10) / 1000.0

        Parsed.new(
          massive_root: massive_root,
          expiration_date: expiration,
          contract_type: contract_type,
          strike: strike,
          symbol: raw.delete_prefix("O:"),
          description: "#{massive_root} #{expiration.iso8601} #{contract_type} #{format_strike(strike)}"
        )
      rescue ArgumentError
        raise Tickrake::Error, "Invalid Massive option ticker `#{ticker}`."
      end

      private

      def format_strike(value)
        value.to_i == value ? value.to_i.to_s : value.to_s
      end
    end
  end
end
