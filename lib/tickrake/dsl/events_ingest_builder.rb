# frozen_string_literal: true

module Tickrake
  module DSL
    class EventsIngestBuilder
      def batch_size(n)
        @batch_size = Integer(n)
      end

      def stale_age(seconds)
        @stale_age_seconds = Integer(seconds)
      end

      def build!(job_name:, config:)
        {
          "batch_size"        => @batch_size || 10,
          "stale_age_seconds" => @stale_age_seconds || 1800
        }
      end
    end
  end
end
