# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSamples
    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, dry_run: false, progress_reporter: nil)
      @tracker = tracker
      @dry_run = dry_run
      @validator = Tickrake::OptionCompactionValidator.new(
        config: config,
        option_root: option_root,
        sample_date: sample_date,
        provider_name: provider_name,
        progress_reporter: progress_reporter
      )
    end

    def run
      validation = nil
      deleted_paths = []
      deletion_errors = []

      validation = @validator.validate
      return validation_result(validation) unless validation.safe_to_delete
      return validation_result(validation, dry_run: true) if @dry_run

      validation.source_paths.each do |path|
        File.delete(path)
        deleted_paths << path
      rescue StandardError => e
        deletion_errors << "Failed to delete source snapshot CSV #{path}: #{e.message}"
        break
      end

      metadata_rows_removed = 0
      if deleted_paths.any?
        metadata_rows_removed = @tracker.delete_file_metadata_paths(deleted_paths)
      end

      validation_result(
        validation,
        dry_run: false,
        deleted_paths: deleted_paths,
        metadata_rows_removed: metadata_rows_removed,
        deletion_errors: deletion_errors
      )
    rescue StandardError => e
      validation_result(
        validation || fallback_result,
        dry_run: @dry_run,
        deleted_paths: deleted_paths,
        metadata_rows_removed: nil,
        deletion_errors: deletion_errors + ["Failed to remove metadata rows for deleted source snapshots: #{e.message}"]
      )
    end

    private

    def validation_result(validation, dry_run: nil, deleted_paths: [], metadata_rows_removed: nil, deletion_errors: [])
      Tickrake::OptionCompactionValidator::Result.new(
        **validation.to_h.merge(
          dry_run: dry_run,
          deleted_paths: deleted_paths,
          metadata_rows_removed: metadata_rows_removed,
          deletion_errors: deletion_errors
        )
      )
    end

    def fallback_result
      Tickrake::OptionCompactionValidator::Result.new(
        safe_to_delete: false,
        provider_name: nil,
        option_root: nil,
        sample_date: nil,
        compacted_path: nil,
        source_paths: [],
        expected_row_count: 0,
        actual_row_count: 0,
        dry_run: @dry_run,
        deleted_paths: [],
        metadata_rows_removed: nil,
        deletion_errors: [],
        errors: []
      )
    end
  end
end
