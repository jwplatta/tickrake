# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSamples
    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, dry_run: false, progress_reporter: nil)
      @config = config
      @tracker = tracker
      @option_root = option_root
      @sample_date = sample_date
      @provider_name = provider_name
      @dry_run = dry_run
      @progress_reporter = progress_reporter
    end

    def run
      validation = nil
      validation = Tickrake::Maintenance::OptionSamples::Processor.new(
        config: @config,
        tracker: @tracker,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        logger: nil
      ).validate(progress_reporter: @progress_reporter)

      return result_for(validation, deleted_paths: [], metadata_rows_removed: nil, deletion_errors: []) unless validation.safe_to_delete
      return result_for(validation, dry_run: true, deleted_paths: [], metadata_rows_removed: nil, deletion_errors: []) if @dry_run

      deleted_paths = []
      deletion_errors = []
      validation.source_paths.each do |path|
        File.delete(path)
        deleted_paths << path
      rescue StandardError => e
        deletion_errors << "Failed to delete source snapshot CSV #{path}: #{e.message}"
        break
      end

      metadata_rows_removed = deleted_paths.empty? ? 0 : @tracker.delete_file_metadata_paths(deleted_paths)
      result_for(validation, deleted_paths: deleted_paths, metadata_rows_removed: metadata_rows_removed, deletion_errors: deletion_errors)
    rescue StandardError => e
      result_for(validation || empty_validation_result, deleted_paths: [], metadata_rows_removed: nil, deletion_errors: [e.message])
    end

    private

    def empty_validation_result
      Tickrake::Maintenance::OptionSamples::ValidationResult.new(
        safe_to_delete: false,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        compacted_path: nil,
        source_paths: [],
        expected_row_count: 0,
        actual_row_count: 0,
        errors: []
      )
    end

    def result_for(validation, dry_run: @dry_run, deleted_paths:, metadata_rows_removed:, deletion_errors:)
      Tickrake::OptionCompactionValidator::Result.new(
        safe_to_delete: validation.safe_to_delete,
        provider_name: validation.provider_name,
        option_root: validation.option_root,
        sample_date: validation.sample_date,
        compacted_path: validation.compacted_path,
        source_paths: validation.source_paths,
        expected_row_count: validation.expected_row_count,
        actual_row_count: validation.actual_row_count,
        dry_run: dry_run,
        deleted_paths: deleted_paths,
        metadata_rows_removed: metadata_rows_removed,
        deletion_errors: deletion_errors,
        errors: validation.errors
      )
    end
  end
end
