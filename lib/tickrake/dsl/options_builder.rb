# frozen_string_literal: true

module Tickrake
  module DSL
    class OptionsBuilder
      def initialize
        @dte_buckets = []
        @settings = {}
      end

      def dte(*args)
        @dte_buckets = case args
                       in [Range => r]  then r.to_a
                       in [Array => a]  then a
                       else args
                       end
      end

      def strikes(range)
        @settings[:strikes] = range
      end

      def include_weeklies(value)
        @settings[:include_weeklies] = value
      end

      def build!
        { dte_buckets: @dte_buckets, settings: @settings }
      end
    end
  end
end
