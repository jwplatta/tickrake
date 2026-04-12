# frozen_string_literal: true

module Tickrake
  module Providers
    class Base
      attr_reader :provider_name, :adapter_name

      def initialize(provider_name:, adapter_name:)
        @provider_name = provider_name
        @adapter_name = adapter_name
      end

      def provider_name
        @provider_name
      end

      def fetch_bars(symbol:, frequency:, start_date:, end_date:, extended_hours:, previous_close:)
        raise NotImplementedError, "#{self.class} must implement #fetch_bars"
      end

      def capabilities
        {}
      end
    end
  end
end
