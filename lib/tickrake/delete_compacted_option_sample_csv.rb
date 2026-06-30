# frozen_string_literal: true

module Tickrake
  class DeleteCompactedOptionSampleCsv
    Result = Struct.new(
      :provider_name,
      :option_root,
      :sample_date,
      :csv_path,
      :remote_uri,
      :dry_run,
      :deleted,
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

      csv_path = @storage_paths.option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: "csv"
      )
      raise Tickrake::Error, "Compacted CSV not found: #{csv_path}" unless File.exist?(csv_path)

      metadata = @tracker.file_metadata(csv_path)
      raise Tickrake::Error, "Compacted CSV metadata not found: #{csv_path}" unless metadata

      remote_uri = metadata["remote_uri"].to_s
      raise Tickrake::Error, "Compacted CSV has not been uploaded to S3: #{csv_path}" if remote_uri.empty?

      remote_object = @archive_service.verify(csv_path)
      local_size = File.size(csv_path)
      if remote_object.size != local_size
        raise Tickrake::Error, "Archived object size mismatch for #{csv_path}: local=#{local_size} remote=#{remote_object.size}"
      end

      return Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        csv_path: csv_path,
        remote_uri: remote_uri,
        dry_run: true,
        deleted: false
      ) if @dry_run

      File.delete(csv_path)
      @tracker.upsert_file_metadata(
        path: csv_path,
        dataset_type: metadata.fetch("dataset_type"),
        provider_name: metadata.fetch("provider_name"),
        ticker: metadata.fetch("ticker"),
        frequency: metadata["frequency"],
        expiration_date: metadata["expiration_date"],
        storage_format: metadata.fetch("storage_format"),
        storage_location: "remote",
        artifact_status: "remote",
        remote_uri: remote_uri,
        source_file_count: metadata["source_file_count"],
        row_count: metadata.fetch("row_count"),
        first_observed_at: metadata["first_observed_at"],
        last_observed_at: metadata["last_observed_at"],
        file_mtime: metadata.fetch("file_mtime"),
        file_size: 0,
        updated_at: Time.now
      )

      Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        csv_path: csv_path,
        remote_uri: remote_uri,
        dry_run: false,
        deleted: true
      )
    end
  end
end
