# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSampleCsvRange
    Result = Struct.new(
      :deleted,
      :skipped_missing,
      :skipped_not_uploaded,
      :backfilled_remote_uri,
      :errors,
      keyword_init: true
    )

    def initialize(config:, tracker:, option_root:, provider_name:, start_date:, end_date:, archive_service: Tickrake::Storage::S3Archive.new(config), dry_run: false, stdout: $stdout, stderr: $stderr)
      @config = config
      @tracker = tracker
      @option_root = option_root.to_s
      @provider_name = provider_name.to_s
      @start_date = start_date
      @end_date = end_date
      @archive_service = archive_service
      @dry_run = dry_run
      @stdout = stdout
      @stderr = stderr
      @storage_paths = Tickrake::Storage::Paths.new(config)
      @output = Tickrake::DeleteCompactedOptionSampleCsvOutput.new(stdout: stdout)
    end

    def run
      deleted = 0
      skipped_missing = 0
      skipped_not_uploaded = 0
      backfilled_remote_uri = 0
      errors = []

      (@start_date..@end_date).each do |sample_date|
        csv_path = csv_path_for(sample_date)

        unless File.exist?(csv_path)
          skipped_missing += 1
          @stdout.puts("Skip #{sample_date.iso8601}: compacted CSV not found at #{csv_path}")
          next
        end

        metadata = @tracker.file_metadata(csv_path)
        if metadata.nil?
          errors << "Date #{sample_date.iso8601}: compacted CSV metadata not found: #{csv_path}"
          next
        end

        begin
          remote_uri, metadata_backfilled = ensure_remote_uri(sample_date: sample_date, csv_path: csv_path, metadata: metadata)
          if remote_uri.nil?
            skipped_not_uploaded += 1
            @stdout.puts("Skip #{sample_date.iso8601}: compacted CSV has not been uploaded to S3")
            next
          end

          backfilled_remote_uri += 1 if metadata_backfilled

          if @dry_run && metadata_backfilled
            @stdout.puts("Would backfill remote_uri for #{sample_date.iso8601}: #{remote_uri}")
          end

          result = if @dry_run && metadata_backfilled
            Tickrake::DeleteCompactedOptionSampleCsv::Result.new(
              provider_name: @provider_name,
              option_root: @option_root,
              sample_date: sample_date,
              csv_path: csv_path,
              remote_uri: remote_uri,
              dry_run: true,
              deleted: false
            )
          else
            Tickrake::DeleteCompactedOptionSampleCsv.new(
              config: @config,
              tracker: @tracker,
              option_root: @option_root,
              sample_date: sample_date,
              provider_name: @provider_name,
              archive_service: @archive_service,
              dry_run: @dry_run
            ).run
          end

          @output.emit(result)
          deleted += 1 if result.deleted
        rescue StandardError => e
          errors << "Date #{sample_date.iso8601}: #{e.message}"
        end
      end

      @stdout.puts("Summary:")
      @stdout.puts("  deleted: #{deleted}")
      @stdout.puts("  skipped_missing: #{skipped_missing}")
      @stdout.puts("  skipped_not_uploaded: #{skipped_not_uploaded}")
      @stdout.puts("  backfilled_remote_uri: #{backfilled_remote_uri}")
      @stdout.puts("  errors: #{errors.length}")
      errors.each { |error| @stderr.puts("  ERROR: #{error}") }

      Result.new(
        deleted: deleted,
        skipped_missing: skipped_missing,
        skipped_not_uploaded: skipped_not_uploaded,
        backfilled_remote_uri: backfilled_remote_uri,
        errors: errors
      )
    end

    private

    def csv_path_for(sample_date)
      @storage_paths.option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: sample_date,
        format: "csv"
      )
    end

    def ensure_remote_uri(sample_date:, csv_path:, metadata:)
      remote_uri = metadata["remote_uri"].to_s
      return [remote_uri, false] unless remote_uri.empty?

      remote_object = @archive_service.verify(csv_path)
      local_size = File.size(csv_path)
      if remote_object.size != local_size
        raise Tickrake::Error, "Archived object size mismatch for #{csv_path}: local=#{local_size} remote=#{remote_object.size}"
      end

      inferred_remote_uri = remote_object.uri
      return [inferred_remote_uri, true] if @dry_run

      @tracker.upsert_file_metadata(
        path: csv_path,
        dataset_type: metadata.fetch("dataset_type"),
        provider_name: metadata.fetch("provider_name"),
        ticker: metadata.fetch("ticker"),
        frequency: metadata["frequency"],
        expiration_date: metadata["expiration_date"],
        storage_format: metadata.fetch("storage_format"),
        storage_location: metadata.fetch("storage_location"),
        artifact_status: "ready_local_and_remote",
        remote_uri: inferred_remote_uri,
        source_file_count: metadata["source_file_count"],
        row_count: metadata.fetch("row_count"),
        first_observed_at: metadata["first_observed_at"],
        last_observed_at: metadata["last_observed_at"],
        file_mtime: metadata.fetch("file_mtime"),
        file_size: metadata.fetch("file_size"),
        updated_at: Time.now
      )
      @stdout.puts("Backfilled remote_uri for #{sample_date.iso8601}: #{inferred_remote_uri}")
      [inferred_remote_uri, true]
    rescue Aws::S3::Errors::ServiceError
      [nil, false]
    end
  end
end
