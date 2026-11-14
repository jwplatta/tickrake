# frozen_string_literal: true

module Tickrake
  module DSL
    class OrderBookContractsBuilder
      def underlying(sym)
        @underlying = sym.to_s
      end

      def option_root(root)
        @option_root = root.to_s.strip.upcase
      end

      def atm_strikes(n)
        @atm_strikes = Integer(n)
      end

      def atm_points(points)
        @atm_points = Float(points)
      end

      def expirations(front:)
        @front_n = Integer(front)
      end

      def re_resolve_interval(minutes)
        @re_resolve_interval_minutes = Integer(minutes)
      end

      def build!
        raise Tickrake::Error, "contracts block requires `underlying`" if @underlying.nil? || @underlying.empty?

        if @atm_points && @atm_points <= 0
          raise Tickrake::Error, "contracts `atm_points` must be positive"
        end

        if @atm_strikes && @atm_strikes <= 0
          raise Tickrake::Error, "contracts `atm_strikes` must be positive"
        end

        Tickrake::OrderBookContractsConfig.new(
          underlying: @underlying,
          option_root: @option_root.to_s.empty? ? nil : @option_root,
          atm_strikes: @atm_points ? @atm_strikes : (@atm_strikes || 5),
          atm_points: @atm_points,
          front_n: @front_n || 2,
          re_resolve_interval_minutes: @re_resolve_interval_minutes || 30
        )
      end
    end
  end
end
