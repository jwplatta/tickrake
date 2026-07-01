# frozen_string_literal: true

RSpec.describe Tickrake::CleanupCompactedOptionSamples do
  def build_config(dir)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      option_snapshot_filename_timezone: "utc",
      s3_archive: Tickrake::S3ArchiveConfig.new(bucket: "tickrake", region: "us-east-1", prefix: "", storage_class: "GLACIER_IR"),
      data_dir: File.join(dir, "data"),
      history_dir: File.join(dir, "data", "history"),
      options_dir: File.join(dir, "data", "options"),
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 0,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )
  end

  def write_compacted_fixture(config)
    sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
    FileUtils.mkdir_p(sample_dir)

    csv_path = File.join(sample_dir, "SPXW_samples_2026-06-26.csv")
    parquet_path = File.join(sample_dir, "SPXW_samples_2026-06-26.parquet")
    File.write(csv_path, "csv-data")
    File.write(parquet_path, "parquet-data")

    { csv_path: csv_path, parquet_path: parquet_path }
  end

  it "verifies both remote artifacts and delegates cleanup steps" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compacted_fixture(config)
      archive_service = instance_double(Tickrake::Storage::S3Archive)
      delete_sources = instance_double(Tickrake::DeleteCompactedOptionSamples)
      delete_csv = instance_double(Tickrake::DeleteCompactedOptionSampleCsv)

      allow(archive_service).to receive(:verify).with(fixture[:csv_path]).and_return(
        Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake", key: "options/a.csv", size: File.size(fixture[:csv_path]))
      )
      allow(archive_service).to receive(:verify).with(fixture[:parquet_path]).and_return(
        Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake", key: "options/a.parquet", size: File.size(fixture[:parquet_path]))
      )
      allow(Tickrake::DeleteCompactedOptionSamples).to receive(:new).and_return(delete_sources)
      allow(delete_sources).to receive(:run).and_return(
        Tickrake::OptionCompactionValidator::Result.new(
          safe_to_delete: true,
          provider_name: "schwab",
          option_root: "SPXW",
          sample_date: Date.new(2026, 6, 26),
          compacted_path: fixture[:csv_path],
          source_paths: %w[/tmp/raw1.csv /tmp/raw2.csv],
          expected_row_count: 2,
          actual_row_count: 2,
          dry_run: nil,
          deleted_paths: %w[/tmp/raw1.csv /tmp/raw2.csv],
          metadata_rows_removed: 2,
          deletion_errors: [],
          errors: []
        )
      )
      allow(Tickrake::DeleteCompactedOptionSampleCsv).to receive(:new).and_return(delete_csv)
      allow(delete_csv).to receive(:run).and_return(
        Tickrake::DeleteCompactedOptionSampleCsv::Result.new(
          provider_name: "schwab",
          option_root: "SPXW",
          sample_date: Date.new(2026, 6, 26),
          csv_path: fixture[:csv_path],
          remote_uri: "s3://tickrake/options/a.csv",
          dry_run: false,
          deleted: true
        )
      )

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab",
        archive_service: archive_service
      ).run

      expect(result.deleted_source_paths).to eq(%w[/tmp/raw1.csv /tmp/raw2.csv])
      expect(result.deleted_csv).to eq(true)
      expect(result.remote_uris).to eq(
        fixture[:csv_path] => "s3://tickrake/options/a.csv",
        fixture[:parquet_path] => "s3://tickrake/options/a.parquet"
      )
    end
  end

  it "fails before deleting anything when the local parquet is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compacted_fixture(config)
      File.delete(fixture[:parquet_path])
      archive_service = instance_double(Tickrake::Storage::S3Archive)

      expect(Tickrake::DeleteCompactedOptionSamples).not_to receive(:new)
      expect(Tickrake::DeleteCompactedOptionSampleCsv).not_to receive(:new)

      expect do
        described_class.new(
          config: config,
          tracker: tracker,
          option_root: "SPXW",
          sample_date: Date.new(2026, 6, 26),
          provider_name: "schwab",
          archive_service: archive_service
        ).run
      end.to raise_error(Tickrake::Error, /Local compacted Parquet not found/)
    end
  end
end
