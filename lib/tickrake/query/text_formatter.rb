# frozen_string_literal: true

module Tickrake
  module Query
    class TextFormatter
      def format(results:, filters:)
        return "No matching datasets found.\n" if results.empty?

        lines = []
        lines << "Query filters: #{format_filters(filters)}"
        results.each do |result|
          if result.dataset_type == "candles"
            lines << "candles provider=#{result.provider_name} ticker=#{result.ticker} frequency=#{result.frequency} coverage=#{result.coverage} rows=#{result.row_count} available=#{result.first_observed_at}..#{result.last_observed_at}"
            lines << "path=#{result.path}"
          else
            lines << "options provider=#{result.provider_name} ticker=#{result.ticker} coverage=#{result.coverage} snapshots=#{result.snapshot_count} available=#{result.first_observed_at}..#{result.last_observed_at}"
            lines << "latest_path=#{result.latest_path}"
          end
        end
        lines.join("\n") + "\n"
      end

      private

      def format_filters(filters)
        filters
          .select { |_key, value| !value.nil? && value != "" }
          .map { |key, value| "#{key}=#{value}" }
          .join(" ")
      end
    end
  end
end
