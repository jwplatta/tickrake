# frozen_string_literal: true

module Tickrake
  module Maintenance
    module OptionSamples
      class SourceSampleCleaner
        def initialize(context:)
          @context = context
        end

        def run(source_paths:, dry_run: false)
          return build_result(source_paths: source_paths, deleted_source_paths: [], metadata_rows_removed: nil, errors: []) if dry_run

          deleted_source_paths = []
          errors = []
          source_paths.each do |path|
            File.delete(path)
            deleted_source_paths << path
          rescue StandardError => e
            errors << "Failed to delete source snapshot CSV #{path}: #{e.message}"
            break
          end

          metadata_rows_removed = deleted_source_paths.empty? ? 0 : @context.tracker.delete_file_metadata_paths(deleted_source_paths)
          build_result(source_paths: source_paths, deleted_source_paths: deleted_source_paths, metadata_rows_removed: metadata_rows_removed, errors: errors)
        rescue StandardError => e
          build_result(source_paths: source_paths, deleted_source_paths: [], metadata_rows_removed: nil, errors: [e.message])
        end

        private

        def build_result(source_paths:, deleted_source_paths:, metadata_rows_removed:, errors:)
          SourceCleanupResult.new(
            success: errors.empty?,
            provider_name: @context.provider_name,
            option_root: @context.option_root,
            sample_date: @context.sample_date,
            source_paths: source_paths,
            deleted_source_paths: deleted_source_paths,
            metadata_rows_removed: metadata_rows_removed,
            errors: errors
          )
        end
      end
    end
  end
end
