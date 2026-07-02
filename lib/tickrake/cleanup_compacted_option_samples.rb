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
      :source_paths,
      :deleted_source_paths,
      :deleted_csv,
      keyword_init: true
    )

    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, archive_service: nil, dry_run: false)
      @config = config
      @tracker = tracker
      @option_root = option_root
      @sample_date = sample_date
      @provider_name = provider_name
      @archive_service = archive_service
      @dry_run = dry_run
      @storage_paths = Tickrake::Storage::Paths.new(config)
    end

    def run
      csv_path = compacted_path("csv")
      parquet_path = compacted_path("parquet")
      raise Tickrake::Error, "Local compacted CSV not found: #{csv_path}" unless File.exist?(csv_path)
      raise Tickrake::Error, "Local compacted Parquet not found: #{parquet_path}" unless File.exist?(parquet_path)

      remote_csv = archive_service.verify(csv_path)
      remote_parquet = archive_service.verify(parquet_path)
      raise Tickrake::Error, "Archived object size mismatch for #{csv_path}: local=#{File.size(csv_path)} remote=#{remote_csv.size}" if remote_csv.size != File.size(csv_path)
      raise Tickrake::Error, "Archived object size mismatch for #{parquet_path}: local=#{File.size(parquet_path)} remote=#{remote_parquet.size}" if remote_parquet.size != File.size(parquet_path)

      delete_sources_result = Tickrake::DeleteCompactedOptionSamples.new(
        config: @config,
        tracker: @tracker,
        option_root: @option_root,
        sample_date: @sample_date,
        provider_name: @provider_name,
        dry_run: @dry_run
      ).run
      raise Tickrake::Error, "Delete-source validation failed: #{delete_sources_result.errors.join('; ')}" unless delete_sources_result.safe_to_delete
      raise Tickrake::Error, "Delete-source errors: #{delete_sources_result.deletion_errors.join('; ')}" unless delete_sources_result.deletion_errors.empty?

      delete_csv_result = Tickrake::DeleteCompactedOptionSampleCsv.new(
        config: @config,
        tracker: @tracker,
        option_root: @option_root,
        sample_date: @sample_date,
        provider_name: @provider_name,
        archive_service: archive_service,
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
        source_paths: delete_sources_result.source_paths,
        deleted_source_paths: delete_sources_result.deleted_paths,
        deleted_csv: delete_csv_result.deleted
      )
    end

    private

    def compacted_path(format)
      @storage_paths.option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: format
      )
    end

    def archive_service
      @archive_service ||= Tickrake::Storage::S3Archive.new(@config, archive_config: @config.s3_archive)
    end
  end
end
