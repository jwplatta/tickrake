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
      csv_path = Tickrake::Storage::Paths.new(@config).option_compacted_sample_path(
        provider: @provider_name,
        root: @option_root,
        sample_date: @sample_date,
        format: "csv"
      )
      raise Tickrake::Error, "Compacted CSV not found: #{csv_path}" unless File.exist?(csv_path)

      if @dry_run
        remote_object = archive_service.verify(csv_path)
        raise Tickrake::Error, "Archived object size mismatch for #{csv_path}: local=#{File.size(csv_path)} remote=#{remote_object.size}" if remote_object.size != File.size(csv_path)
        return Result.new(
          provider_name: @provider_name,
          option_root: @option_root,
          sample_date: @sample_date,
          csv_path: csv_path,
          remote_uri: remote_object.uri,
          dry_run: true,
          deleted: false
        )
      end

      result = Tickrake::Maintenance::OptionSamples::Processor.new(
        config: @config,
        tracker: @tracker,
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        logger: nil,
        archive_services: { "s3_archive" => archive_service }
      ).archive(destination_name: "s3_archive", artifacts: ["csv"], retain_local: { "csv" => false })
      raise Tickrake::Error, result.errors.join("; ") unless result.successful?

      artifact = result.artifact_results.first
      Result.new(
        provider_name: @provider_name,
        option_root: @option_root,
        sample_date: @sample_date,
        csv_path: csv_path,
        remote_uri: artifact[:remote_uri],
        dry_run: false,
        deleted: !artifact[:retained_local]
      )
    end

    private

    def archive_service
      @archive_service ||= Tickrake::Storage::S3Archive.new(@config, archive_config: @config.s3_archive)
    end
  end
end
