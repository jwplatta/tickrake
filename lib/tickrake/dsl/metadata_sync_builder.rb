# frozen_string_literal: true

module Tickrake
  module DSL
    class MetadataSyncBuilder
      def batch_size(n)
        @batch_size = Integer(n)
      end

      def build!
        { "batch_size" => @batch_size || 500 }
      end
    end
  end
end
