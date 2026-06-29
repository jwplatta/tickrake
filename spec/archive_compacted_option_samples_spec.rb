# frozen_string_literal: true

RSpec.describe Tickrake::ArchiveCompactedOptionSamples do
  def build_config(dir, with_archive: true)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      option_snapshot_filename_timezone: "utc",
      s3_archive: with_archive ? Tickrake::S3ArchiveConfig.new(bucket: "tickrake", region: "us-east-1", prefix: "", storage_class: "GLACIER_IR") : nil,
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

  def write_compacted_fixture(config:)
    sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
    FileUtils.mkdir_p(sample_dir)
    csv_path = File.join(sample_dir, "SPXW_samples_2026-06-26.csv")
    parquet_path = File.join(sample_dir, "SPXW_samples_2026-06-26.parquet")
    File.write(csv_path, "csv-data")
    File.write(parquet_path, "parquet-data")
    { csv: csv_path, parquet: parquet_path }
  end

  def insert_metadata(tracker, paths)
    [
      [paths[:csv], "options_compacted_csv", "csv"],
      [paths[:parquet], "options_compacted_parquet", "parquet"]
    ].each do |path, dataset_type, format|
      tracker.upsert_file_metadata(
        path: path,
        dataset_type: dataset_type,
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: nil,
        storage_format: format,
        storage_location: "local",
        artifact_status: "ready_local",
        remote_uri: nil,
        source_file_count: 2,
        row_count: 2,
        first_observed_at: "2026-06-26T14:30:00Z",
        last_observed_at: "2026-06-26T14:35:00Z",
        file_mtime: File.mtime(path).to_i,
        file_size: File.size(path),
        updated_at: Time.now
      )
    end
  end

  it "uploads both compacted artifacts and records remote metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      paths = write_compacted_fixture(config: config)
      insert_metadata(tracker, paths)
      archive_service = instance_double(Tickrake::Storage::S3Archive)

      allow(archive_service).to receive(:uri_for) { |path| "s3://tickrake/#{path.split('/data/').last}" }
      allow(archive_service).to receive(:upload)
      allow(archive_service).to receive(:verify) do |path|
        key = path.split("/data/").last
        Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake", key: key, size: File.size(path))
      end

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab",
        archive_service: archive_service
      ).run

      expect(result.archived_paths).to eq([paths[:csv], paths[:parquet]])
      expect(tracker.file_metadata(paths[:csv])["remote_uri"]).to eq("s3://tickrake/options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv")
      expect(tracker.file_metadata(paths[:csv])["artifact_status"]).to eq("ready_local_and_remote")
      expect(tracker.file_metadata(paths[:parquet])["remote_uri"]).to eq("s3://tickrake/options/schwab/2026/06/26/SPXW_samples_2026-06-26.parquet")
      expect(tracker.file_metadata(paths[:parquet])["artifact_status"]).to eq("ready_local_and_remote")
    end
  end

  it "fails when the compacted csv is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      paths = write_compacted_fixture(config: config)
      File.delete(paths[:csv])

      expect do
        described_class.new(
          config: config,
          tracker: tracker,
          option_root: "SPXW",
          sample_date: Date.new(2026, 6, 26),
          provider_name: "schwab",
          archive_service: instance_double(Tickrake::Storage::S3Archive)
        ).run
      end.to raise_error(Tickrake::Error, /Compacted artifact not found: .*SPXW_samples_2026-06-26\.csv/)
    end
  end

  it "fails when the compacted parquet is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      paths = write_compacted_fixture(config: config)
      File.delete(paths[:parquet])

      expect do
        described_class.new(
          config: config,
          tracker: tracker,
          option_root: "SPXW",
          sample_date: Date.new(2026, 6, 26),
          provider_name: "schwab",
          archive_service: instance_double(Tickrake::Storage::S3Archive)
        ).run
      end.to raise_error(Tickrake::Error, /Compacted artifact not found: .*SPXW_samples_2026-06-26\.parquet/)
    end
  end

  it "reports s3 uris in dry-run mode without uploading or mutating metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      paths = write_compacted_fixture(config: config)
      insert_metadata(tracker, paths)
      archive_service = instance_double(Tickrake::Storage::S3Archive)

      allow(archive_service).to receive(:uri_for) { |path| "s3://tickrake/#{path.split('/data/').last}" }
      allow(archive_service).to receive(:upload)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab",
        archive_service: archive_service,
        dry_run: true
      ).run

      expect(result.dry_run).to eq(true)
      expect(archive_service).not_to have_received(:upload)
      expect(tracker.file_metadata(paths[:csv])["remote_uri"]).to be_nil
      expect(tracker.file_metadata(paths[:csv])["artifact_status"]).to eq("ready_local")
    end
  end
end
