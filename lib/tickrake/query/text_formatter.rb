# frozen_string_literal: true

module Tickrake
  module Query
    class TextFormatter
      FREQUENCY_ORDER = {
        "1min" => 0,
        "5min" => 1,
        "10min" => 2,
        "15min" => 3,
        "30min" => 4,
        "day" => 5,
        "week" => 6,
        "month" => 7
      }.freeze

      def format(results:, filters:)
        return "No matching datasets found.\n" if results.empty?

        lines = []
        lines << "Filters: #{format_filters(filters)}"

        grouped_results(results).each_with_index do |((provider_name, dataset_type, ticker), grouped), index|
          lines << "" if index.positive?
          lines << "Provider: #{provider_name}"
          lines << "Type: #{dataset_type}"
          lines << "Ticker: #{ticker}"
          lines << ""

          if dataset_type == "candles"
            grouped.sort_by { |result| frequency_rank(result.frequency) }.each do |result|
              lines << "- #{result.frequency}"
              lines << "  coverage: #{result.coverage}"
              lines << "  rows: #{result.row_count}"
              lines << "  available: #{result.first_observed_at} -> #{result.last_observed_at}"
              lines << "  path: #{result.path}"
              lines << ""
            end
          else
            grouped.each do |result|
              lines << "- #{result.root_symbol} exp #{result.expiration_date}"
              lines << "  sample_datetime: #{result.sample_datetime}"
              lines << "  file_path: #{result.file_path}"
              lines << ""
            end
          end
        end

        lines.pop while lines.last == ""
        lines.join("\n") + "\n"
      end

      private

      def format_filters(filters)
        filters
          .select { |_key, value| !value.nil? && value != "" }
          .map { |key, value| "#{key}=#{value}" }
          .join(" ")
      end

      def grouped_results(results)
        results
          .group_by { |result| [result.provider_name, result.dataset_type, result.ticker] }
          .sort_by { |(provider_name, dataset_type, ticker), _| [provider_name, dataset_type, ticker] }
      end

      def frequency_rank(frequency)
        FREQUENCY_ORDER.fetch(frequency, 100)
      end
    end
  end
end
