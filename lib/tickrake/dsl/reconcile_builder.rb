# frozen_string_literal: true

module Tickrake
  module DSL
    class ReconcileBuilder
      def initialize
        @providers = nil
      end

      def providers(*args)
        @providers = args.flatten.map(&:to_s)
      end

      def build!
        { "providers" => @providers }
      end
    end
  end
end
