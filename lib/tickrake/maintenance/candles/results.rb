# frozen_string_literal: true

module Tickrake
  module Maintenance
    module Candles
      CompactResult = Struct.new(
        :success,
        :provider_name,
        :symbol,
        :frequency,
        :row_count,
        :years_written,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success && Array(errors).empty?
        end

        def skipped?
          success && row_count.to_i.zero?
        end

        def artifacts_written
          Array(years_written).map(&:to_s)
        end
      end

      ArchiveResult = Struct.new(
        :success,
        :provider_name,
        :symbol,
        :frequency,
        :artifact_results,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success && Array(errors).empty?
        end

        def artifacts_written
          Array(artifact_results).map { |r| r[:path] }
        end

        def archived_paths
          artifacts_written
        end
      end
    end
  end
end
