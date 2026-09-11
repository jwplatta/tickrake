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

      def datastore(name)
        @datastore_name = name.to_s
      end

      def build!(job_name:, config:)
        if @datastore_name && !config.datastores.key?(@datastore_name)
          raise Tickrake::Error, "events_ingest job `#{job_name}` datastore `#{@datastore_name}` is not configured"
        end

        {
          "batch_size"        => @batch_size || 10,
          "stale_age_seconds" => @stale_age_seconds || 1800,
          "datastore_name"    => @datastore_name
        }
      end
    end
  end
end
