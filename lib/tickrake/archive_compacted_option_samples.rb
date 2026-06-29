# frozen_string_literal: true

module Tickrake
  class ArchiveCompactedOptionSamples
    Result = Struct.new(
      :provider_name,
      :option_root,
      :sample_date,
      :dry_run,
      :archived_paths,
      :remote_uris,
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

      paths = compacted_paths
      missing_path = paths.find { |path| !File.exist?(path) }
      raise Tickrake::Error, "Compacted artifact not found: #{missing_path}" if missing_path

      remote_uris = paths.each_with_object({}) do |path, memo|
        memo[path] = @archive_service.uri_for(path)
      end

      return build_result(paths, remote_uris) if @dry_run

      paths.each do |path|
        @archive_service.upload(path)
        remote_object = @archive_service.verify(path)
        local_size = File.size(path)
        if remote_object.size != local_size
          raise Tickrake::Error,
                "Archived object size mismatch for #{path}: local=#{local_size} remote=#{remote_object.size}"
        end

        update_metadata_for(path, remote_uri: remote_object.uri)
      end

      build_result(paths, remote_uris)
    end

    private

    def build_result(paths, remote_uris)
      Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        dry_run: @dry_run,
        archived_paths: paths,
        remote_uris: remote_uris
      )
    end

    def compacted_paths
      %w[csv parquet].map do |format|
        @storage_paths.option_compacted_sample_path(
          provider: @provider_name,
          root: @option_root,
          sample_date: @sample_date,
          format: format
        )
      end
    end

    def update_metadata_for(path, remote_uri:)
      metadata = @tracker.file_metadata(path)
      raise Tickrake::Error, "Compacted artifact metadata not found: #{path}" unless metadata

      @tracker.upsert_file_metadata(
        path: path,
        dataset_type: metadata.fetch("dataset_type"),
        provider_name: metadata.fetch("provider_name"),
        ticker: metadata.fetch("ticker"),
        frequency: metadata["frequency"],
        expiration_date: metadata["expiration_date"],
        storage_format: metadata.fetch("storage_format"),
        storage_location: "local",
        artifact_status: "ready_local_and_remote",
        remote_uri: remote_uri,
        source_file_count: metadata["source_file_count"],
        row_count: metadata.fetch("row_count"),
        first_observed_at: metadata["first_observed_at"],
        last_observed_at: metadata["last_observed_at"],
        file_mtime: File.mtime(path).to_i,
        file_size: File.size(path),
        updated_at: Time.now
      )
    end
  end
end
