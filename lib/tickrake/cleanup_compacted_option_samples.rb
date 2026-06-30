# frozen_string_literal: true

module Tickrake
  class CleanupCompactedOptionSamples
    Result = Struct.new(
      :provider_name,
      :option_root,
      :sample_date,
      :dry_run,
      :csv_path,
      :parquet_path,
      :remote_uris,
      :deleted_source_paths,
      :deleted_csv,
      keyword_init: true
    )

    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, archive_service: Tickrake::Storage::S3Archive.new(config), dry_run: false)
      @config = config
      @tracker = tracker
      @option_root = option_root.to_s
      @sample_date = sample_date
      @provider_name = provider_name.to_s
      @archive_service = archive_service
      @dry_run = dry_run
      @storage_paths = Tickrake::Storage::Paths.new(config)
    end

    def run
      raise Tickrake::Error, "S3 archive is not configured." unless @config.s3_archive

      csv_path = compacted_path(format: "csv")
      parquet_path = compacted_path(format: "parquet")

      raise Tickrake::Error, "Local compacted CSV not found: #{csv_path}" unless File.exist?(csv_path)
      raise Tickrake::Error, "Local compacted Parquet not found: #{parquet_path}" unless File.exist?(parquet_path)

      remote_csv = verify_remote_match!(csv_path)
      remote_parquet = verify_remote_match!(parquet_path)

      delete_sources_result = Tickrake::DeleteCompactedOptionSamples.new(
        config: @config,
        tracker: @tracker,
        option_root: @option_root,
        sample_date: @sample_date,
        provider_name: @provider_name,
        dry_run: @dry_run
      ).run
      raise Tickrake::Error, "Delete-source validation failed: #{delete_sources_result.errors.join('; ')}" unless delete_sources_result.safe_to_delete
      unless delete_sources_result.deletion_errors.empty?
        raise Tickrake::Error, "Delete-source errors: #{delete_sources_result.deletion_errors.join('; ')}"
      end

      delete_csv_result = Tickrake::DeleteCompactedOptionSampleCsv.new(
        config: @config,
        tracker: @tracker,
        option_root: @option_root,
        sample_date: @sample_date,
        provider_name: @provider_name,
        archive_service: @archive_service,
        dry_run: @dry_run
      ).run

      Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        dry_run: @dry_run,
        csv_path: csv_path,
        parquet_path: parquet_path,
        remote_uris: {
          csv_path => remote_csv.uri,
          parquet_path => remote_parquet.uri
        },
        deleted_source_paths: delete_sources_result.deleted_paths,
        deleted_csv: delete_csv_result.deleted
      )
    end

    private

    def compacted_path(format:)
      @storage_paths.option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: format
      )
    end

    def verify_remote_match!(path)
      remote_object = @archive_service.verify(path)
      local_size = File.size(path)
      if remote_object.size != local_size
        raise Tickrake::Error,
              "Archived object size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
      end

      remote_object
    end
  end
end
