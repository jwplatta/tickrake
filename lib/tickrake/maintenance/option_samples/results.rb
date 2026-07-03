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
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
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

      ArchiveResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :artifact_results,
        :errors,
        keyword_init: true
      ) do
        def successful?
          success
        end

        def archived_paths
          artifact_results.map { |result| result.fetch(:path) }
        end

        def remote_uris
          artifact_results.each_with_object({}) do |result, memo|
            memo[result.fetch(:path)] = result.fetch(:remote_uri)
          end
        end
      end

      RetentionResult = Struct.new(
        :success,
        :provider_name,
        :option_root,
        :sample_date,
        :retained_local,
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
