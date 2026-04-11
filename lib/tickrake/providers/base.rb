# frozen_string_literal: true

module Tickrake
  module Providers
    class Base
      def provider_name
        raise NotImplementedError, "#{self.class} must implement #provider_name"
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
