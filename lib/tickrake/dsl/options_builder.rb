# frozen_string_literal: true

module Tickrake
  module DSL
    class OptionsBuilder
      def initialize
        @dte_buckets = []
      end

      def dte(*args)
        @dte_buckets = case args
                       in [Range => r]  then r.to_a
                       in [Array => a]  then a
                       else args
                       end
      end

      def build!
        { dte_buckets: @dte_buckets }
      end
    end
  end
end
