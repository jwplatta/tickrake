# frozen_string_literal: true

module Tickrake
  module DSL
    class UniverseBuilder
      attr_reader :entries

      def initialize
        @entries = []
      end

      def ticker(symbol, option_root: nil, option_roots: [])
        @entries << Tickrake::UniverseEntry.new(
          symbol: symbol.to_s,
          option_root: option_root&.to_s,
          option_roots: Array(option_roots).map(&:to_s),
          start_date: nil,
          need_extended_hours_data: false,
          need_previous_close: false
        )
      end
    end
  end
end
