# frozen_string_literal: true

module Tickrake
  module DSL
    class IntradayPublishBuilder
      def initialize
        @datastore_name = nil
        @clear_at = "00:00"
      end

      def datastore(name)
        @datastore_name = name.to_s
      end

      def clear_at(time)
        @clear_at = time ? parse_clock(time) : nil
      end

      def build!
        raise Tickrake::Error, "intraday_publish block requires datastore" if @datastore_name.nil?

        {
          "datastore_name" => @datastore_name,
          "clear_at" => @clear_at
        }
      end

      private

      def parse_clock(value)
        match = /\A(\d{1,2}):(\d{2})\z/.match(value.to_s)
        raise Tickrake::Error, "Invalid clock value for clear_at: #{value.inspect}" unless match

        sprintf("%02d:%02d", Integer(match[1], 10), Integer(match[2], 10))
      end
    end
  end
end
