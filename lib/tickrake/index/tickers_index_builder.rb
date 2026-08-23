# frozen_string_literal: true

module Tickrake
  module Index
    class TickersIndexBuilder
      SCHEMA_VERSION = 1

      def initialize(tracker:)
        @tracker = tracker
      end

      def build(provider:)
        {
          "schema_version" => SCHEMA_VERSION,
          "provider" => provider,
          "updated_at" => Time.now.utc.iso8601,
          "roots" => @tracker.known_roots(provider_name: provider)
        }
      end
    end
  end
end
