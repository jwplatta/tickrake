# frozen_string_literal: true

module Tickrake
  module DSL
    class IntradayPublishBuilder
      def datastore(name)
        @datastore_name = name.to_s
      end

      def build!
        raise Tickrake::Error, "intraday_publish block requires datastore" if @datastore_name.nil?

        { "datastore_name" => @datastore_name }
      end
    end
  end
end
