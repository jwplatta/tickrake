# frozen_string_literal: true

module Tickrake
  module Data
    Bar = Struct.new(
      :datetime,
      :open,
      :high,
      :low,
      :close,
      :volume,
      :source,
      :symbol,
      :frequency,
      keyword_init: true
    ) do
      def utc_datetime
        case datetime
        when Time
          datetime.utc
        else
          Time.iso8601(datetime.to_s).utc
        end
      end
    end
  end
end
