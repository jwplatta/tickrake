# frozen_string_literal: true

module Tickrake
  module Index
    class RootIndexBuilder
      def initialize(tracker:, options_dir:)
        @tracker = tracker
        @options_dir = options_dir
      end

      def build(provider:, root:)
        {
          "provider" => provider,
          "root" => root,
          "updated_at" => Time.now.utc.iso8601,
          "intraday" => build_intraday(provider, root)
        }
      end

      private

      def build_intraday(provider, root)
        rows = @tracker.intraday_index_rows(provider_name: provider, root: root)
        return nil if rows.empty?

        first = rows.first
        files = rows.map do |row|
          {
            "expiration_date" => row.fetch("expiration_date"),
            "format" => "csv",
            "uri" => "file://#{row.fetch("path")}",
            "row_count" => row.fetch("row_count")
          }
        end

        {
          "sample_date" => first.fetch("sample_date"),
          "sampled_at" => first.fetch("sampled_at"),
          "status" => "complete",
          "files" => files
        }
      end
    end
  end
end
