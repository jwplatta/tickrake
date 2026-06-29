# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSamples
    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, dry_run: false, progress_reporter: nil, archive_service: nil)
      @config = config
      @tracker = tracker
      @dry_run = dry_run
      @archive_service = archive_service || (@config.s3_archive && Tickrake::Storage::S3Archive.new(@config))
      @storage_paths = Tickrake::Storage::Paths.new(@config)
      @validator = Tickrake::OptionCompactionValidator.new(
        config: @config,
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
      validation = validate_remote_archive(validation)
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

    def validate_remote_archive(validation)
      return validation unless validation.safe_to_delete
      return validation unless @config.s3_archive

      errors = compacted_formats.filter_map do |format|
        compacted_path = @storage_paths.option_compacted_sample_path(
          provider: validation.provider_name,
          root: validation.option_root,
          sample_date: validation.sample_date,
          format: format
        )
        validate_remote_artifact(path: compacted_path, format: format)
      rescue Tickrake::Error, Aws::S3::Errors::ServiceError => e
        "Remote archive verification failed for compacted #{format.upcase}: #{e.message}"
      end

      return validation if errors.empty?

      Tickrake::OptionCompactionValidator::Result.new(**validation.to_h.merge(safe_to_delete: false, errors: validation.errors + errors))
    end

    def validate_remote_artifact(path:, format:)
      metadata = @tracker.file_metadata(path)
      return "Compacted #{format.upcase} metadata not found: #{path}" unless metadata
      return "Compacted #{format.upcase} metadata is missing remote_uri: #{path}" if metadata["remote_uri"].to_s.strip.empty?

      remote_object = @archive_service.verify(path)
      local_size = File.size(path)
      return if remote_object.size == local_size

      "Compacted #{format.upcase} remote size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
    end

    def compacted_formats
      %w[csv parquet]
    end
  end
end
