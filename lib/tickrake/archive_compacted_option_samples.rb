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

    def initialize(config:, tracker:, option_root:, sample_date:, provider_name:, archive_service: nil, dry_run: false)
      @config = config
      @tracker = tracker
      @option_root = option_root
      @sample_date = sample_date
      @provider_name = provider_name
      @archive_service = archive_service
      @dry_run = dry_run
    end

    def run
      paths = %w[csv parquet].map { |artifact| compacted_path_for(artifact) }
      paths.each do |path|
        raise Tickrake::Error, "Compacted artifact not found: #{path}" unless File.exist?(path)
      end

      if @dry_run
        remote_uris = paths.to_h { |path| [path, archive_service.uri_for(path)] }
        return Result.new(
          provider_name: @provider_name,
          option_root: @option_root,
          sample_date: @sample_date,
          dry_run: true,
          archived_paths: paths,
          remote_uris: remote_uris
        )
      end

      result = processor.archive(destination_name: "s3_archive", artifacts: %w[csv parquet], retain_local: { "csv" => true, "parquet" => true })
      raise Tickrake::Error, result.errors.join("; ") unless result.successful?

      Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        dry_run: false,
        archived_paths: result.archived_paths,
        remote_uris: result.artifact_results.each_with_object({}) { |artifact, memo| memo[artifact[:path]] = artifact[:remote_uri] }
      )
    end

    private

    def processor
      @processor ||= Tickrake::Maintenance::OptionSamples::Processor.new(
        config: @config,
        tracker: @tracker,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        logger: nil,
        archive_services: { "s3_archive" => archive_service }
      )
    end

    def compacted_path_for(artifact)
      Tickrake::Storage::Paths.new(@config).option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: artifact
      )
    end

    def archive_service
      @archive_service ||= Tickrake::Storage::S3Archive.new(@config, archive_config: @config.s3_archive)
    end
  end
end
