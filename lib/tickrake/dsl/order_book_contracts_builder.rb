# frozen_string_literal: true

module Tickrake
  module DSL
    class OrderBookContractsBuilder
      def underlying(sym)
        @underlying = sym.to_s
      end

      def atm_strikes(n)
        @atm_strikes = Integer(n)
      end

      def expirations(front:)
        @front_n = Integer(front)
      end

      def re_resolve_interval(minutes)
        @re_resolve_interval_minutes = Integer(minutes)
      end

      def build!
        raise Tickrake::Error, "order_book contracts block requires `underlying`" if @underlying.nil? || @underlying.empty?

        Tickrake::OrderBookContractsConfig.new(
          underlying: @underlying,
          atm_strikes: @atm_strikes || 5,
          front_n: @front_n || 2,
          re_resolve_interval_minutes: @re_resolve_interval_minutes || 30
        )
      end
    end
  end
end
