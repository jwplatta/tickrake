# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      ValidationResult = Struct.new(
        :safe_to_delete,
        :provider_name,
        :option_root,
        :sample_date,
        :compacted_path,
        :source_paths,
        :expected_row_count,
        :actual_row_count,
        :errors,
        keyword_init: true
      )

      CompactResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :artifacts_written,
        :row_count,
        :source_file_count,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end

        def skipped?
          success && Array(artifacts_written).empty?
        end
      end

      SourceCleanupResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :source_paths,
        :deleted_source_paths,
        :metadata_rows_removed,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end
      end
    end
  end
end
