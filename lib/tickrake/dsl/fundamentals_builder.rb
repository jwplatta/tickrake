# frozen_string_literal: true

module Tickrake
  module DSL
    class FundamentalsBuilder
      def batch_size(n)
        @batch_size = Integer(n)
      end

      def build!
        { "batch_size" => @batch_size || 50 }
      end
    end
  end
end
