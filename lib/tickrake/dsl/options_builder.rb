# frozen_string_literal: true

module Tickrake
  module DSL
    class OptionsBuilder
      DEFAULT_BATCH_SIZE = 1

      def initialize
        @dte_buckets = []
        @batch_size = nil
      end

      def dte(*args)
        @dte_buckets = case args
                       in [Range => r]  then r.to_a
                       in [Array => a]  then a
                       else args
                       end
      end

      def batch_size(n)
        value = Integer(n)
        raise ArgumentError, "batch_size must be a positive integer" if value < 1

        @batch_size = value
      end

      def build!
        {
          dte_buckets: @dte_buckets,
          batch_size: @batch_size || DEFAULT_BATCH_SIZE
        }
      end
    end
  end
end
