# frozen_string_literal: true

module Tickrake
  module DSL
    class EconomicEventsBuilder
      def lookback_days(n)
        @lookback_days = Integer(n)
      end

      def lookahead_days(n)
        @lookahead_days = Integer(n)
      end

      def categories(*args)
        @categories = args.flatten.map(&:to_s)
      end

      def build!
        {
          "lookback_days"  => @lookback_days  || 30,
          "lookahead_days" => @lookahead_days || 90,
          "categories"     => @categories     || %w[economic earnings fomc]
        }
      end
    end
  end
end
