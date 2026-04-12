# frozen_string_literal: true

module Tickrake
  module Query
    class JsonFormatter
      def format(results:, filters:)
        JSON.pretty_generate(
          {
            filters: filters.transform_values { |value| value.respond_to?(:iso8601) ? value.iso8601 : value },
            results: results.map do |result|
              hash = result.to_h
              hash.transform_values { |value| value.respond_to?(:iso8601) ? value.iso8601 : value }
            end
          }
        ) + "\n"
      end
    end
  end
end
