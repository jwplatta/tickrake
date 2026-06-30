# frozen_string_literal: true

RSpec.describe Tickrake::DeleteCompactedOptionSampleCsvRange do
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

  def write_csv(config:, sample_date:)
    sample_dir = File.join(config.options_dir, "schwab", sample_date.strftime("%Y"), sample_date.strftime("%m"), sample_date.strftime("%d"))
    FileUtils.mkdir_p(sample_dir)
    path = File.join(sample_dir, "SPXW_samples_#{sample_date.iso8601}.csv")
    File.write(path, "csv-data")
    path
  end

  def insert_metadata(tracker, path:, remote_uri:, artifact_status: "ready_local")
    tracker.upsert_file_metadata(
      path: path,
      dataset_type: "options_compacted_csv",
      provider_name: "schwab",
      ticker: "SPXW",
      frequency: nil,
      expiration_date: nil,
      storage_format: "csv",
      storage_location: "local",
      artifact_status: artifact_status,
      remote_uri: remote_uri,
      source_file_count: 2,
      row_count: 2,
      first_observed_at: "2026-06-26T14:30:00Z",
      last_observed_at: "2026-06-26T14:35:00Z",
      file_mtime: File.mtime(path).to_i,
      file_size: File.size(path),
      updated_at: Time.now
    )
  end

  it "backfills a missing remote_uri when the csv is already in s3 and then deletes it" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      sample_date = Date.new(2026, 6, 26)
      csv_path = write_csv(config: config, sample_date: sample_date)
      insert_metadata(tracker, path: csv_path, remote_uri: nil, artifact_status: "ready_local")
      archive_service = instance_double(Tickrake::Storage::S3Archive)
      allow(archive_service).to receive(:verify).with(csv_path).and_return(
        Tickrake::Storage::S3Archive::RemoteObject.new(
          bucket: "tickrake",
          key: "options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv",
          size: File.size(csv_path)
        )
      )
      stdout = StringIO.new

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        provider_name: "schwab",
        start_date: sample_date,
        end_date: sample_date,
        archive_service: archive_service,
        stdout: stdout,
        stderr: StringIO.new
      ).run

      expect(result.deleted).to eq(1)
      expect(result.backfilled_remote_uri).to eq(1)
      expect(File.exist?(csv_path)).to eq(false)
      expect(tracker.file_metadata(csv_path)["artifact_status"]).to eq("remote")
      expect(tracker.file_metadata(csv_path)["remote_uri"]).to eq("s3://tickrake/options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv")
    end
  end

  it "dry-runs a remote_uri backfill and delete without mutating metadata or files" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      sample_date = Date.new(2026, 6, 26)
      csv_path = write_csv(config: config, sample_date: sample_date)
      insert_metadata(tracker, path: csv_path, remote_uri: nil, artifact_status: "ready_local")
      archive_service = instance_double(Tickrake::Storage::S3Archive)
      allow(archive_service).to receive(:verify).with(csv_path).and_return(
        Tickrake::Storage::S3Archive::RemoteObject.new(
          bucket: "tickrake",
          key: "options/schwab/2026/06/26/SPXW_samples_2026-06-26.csv",
          size: File.size(csv_path)
        )
      )

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        provider_name: "schwab",
        start_date: sample_date,
        end_date: sample_date,
        archive_service: archive_service,
        dry_run: true,
        stdout: StringIO.new,
        stderr: StringIO.new
      ).run

      expect(result.deleted).to eq(0)
      expect(result.backfilled_remote_uri).to eq(1)
      expect(File.exist?(csv_path)).to eq(true)
      expect(tracker.file_metadata(csv_path)["remote_uri"]).to be_nil
      expect(tracker.file_metadata(csv_path)["artifact_status"]).to eq("ready_local")
    end
  end
end
