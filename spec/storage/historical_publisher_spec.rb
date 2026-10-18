# frozen_string_literal: true

require "spec_helper"

RSpec.describe Tickrake::Storage::HistoricalPublisher do
  let(:tmpdir) { Dir.mktmpdir("tickrake_historical_test") }
  let(:data_dir) { File.join(tmpdir, "data") }
  let(:db_path) { File.join(tmpdir, "tickrake.sqlite3") }

  let(:datastore_config) do
    Tickrake::DatastoreConfig.new(
      name: "minio_historical",
      type: "minio",
      bucket: "tickrake-historical",
      endpoint: "http://localhost:9000",
      access_key_id: "minioadmin",
      secret_access_key: "minioadmin",
      force_path_style: true
    )
  end

  let(:config) do
    instance_double(
      Tickrake::Config,
      data_dir: data_dir,
      historical_datastore: "minio_historical",
      s3_archive: datastore_config,
      datastore: datastore_config
    )
  end

  let(:logger) { instance_double(Logger, info: nil, warn: nil, error: nil) }
  let(:runtime) { instance_double(Tickrake::Runtime, config: config, logger: logger) }
  let(:mock_s3) { instance_double(Aws::S3::Client) }

  before do
    FileUtils.mkdir_p(data_dir)
    Tickrake::Tracker.migrate!(db_path)
  end

  after do
    FileUtils.rm_rf(tmpdir)
  end

  subject(:publisher) { described_class.new(runtime: runtime, datastore_name: "minio_historical") }

  it "extracts metadata correctly across all dataset types" do
    m1 = publisher.extract_metadata("options", File.join(data_dir, "options/massive/2025/03/03/SPXW_samples_2025-03-03.parquet"))
    expect(m1).to eq(provider: "massive", symbol: "SPXW", date_or_year: "2025-03-03", frequency: nil)

    m2 = publisher.extract_metadata("candles", File.join(data_dir, "candles/ibkr/5min/2025/SPX.parquet"))
    expect(m2).to eq(provider: "ibkr", symbol: "SPX", date_or_year: "2025", frequency: "5min")

    m3 = publisher.extract_metadata("level_one", File.join(data_dir, "level_one/schwab/2026/10/02/^ES_004524Z.parquet"))
    expect(m3).to eq(provider: "schwab", symbol: "^ES", date_or_year: "2026-10-02", frequency: nil)

    m4 = publisher.extract_metadata("fundamentals", File.join(data_dir, "fundamentals/schwab/2026/10/15.parquet"))
    expect(m4).to eq(provider: "schwab", symbol: "ALL", date_or_year: "2026-10-15", frequency: nil)

    m5 = publisher.extract_metadata("economic_events", File.join(data_dir, "economic_events/fred/fomc/2026/11/24.parquet"))
    expect(m5).to eq(provider: "fred", symbol: "fomc", date_or_year: "2026-11-24", frequency: nil)
  end

  it "scans, uploads changed files, records in ledger, and writes manifests" do
    # Create sample parquet file
    candle_dir = File.join(data_dir, "candles", "ibkr", "5min", "2025")
    FileUtils.mkdir_p(candle_dir)
    parquet_file = File.join(candle_dir, "SPX.parquet")
    File.write(parquet_file, "PAR1mockparquetcontent")

    # Mock S3 calls
    uploaded_files = {}
    uploaded_manifests = {}
    store = publisher.instance_variable_get(:@store)
    allow(store).to receive(:object_exists?).and_return(false)
    allow(store).to receive(:upload_file) do |path, key:|
      uploaded_files[key] = path
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: key, size: File.size(path))
    end
    allow(store).to receive(:upload_content) do |key, content|
      uploaded_manifests[key] = JSON.parse(content)
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: key, size: content.bytesize)
    end
    allow(publisher).to receive(:count_rows).and_return(42)

    # First run: should upload
    summary = publisher.scan_and_publish
    expect(summary[:files_scanned]).to eq(1)
    expect(summary[:files_uploaded]).to eq(1)
    expect(uploaded_files.keys).to include("candles/ibkr/5min/2025/SPX.parquet")
    expect(uploaded_manifests.keys).to include("manifests/candles.json")
    expect(uploaded_manifests["manifests/candles.json"]["total_rows"]).to eq(42)

    # Verify ledger record in database
    ledger_entry = Tickrake::Models::HistoricalFileLedger.find_by(remote_key: "candles/ibkr/5min/2025/SPX.parquet")
    expect(ledger_entry).not_to be_nil
    expect(ledger_entry.dataset_type).to eq("candles")
    expect(ledger_entry.symbol).to eq("SPX")
    expect(ledger_entry.frequency).to eq("5min")
    expect(ledger_entry.row_count).to eq(42)

    # Second run without modifying file: should skip upload (idempotent)
    summary2 = publisher.scan_and_publish
    expect(summary2[:files_scanned]).to eq(1)
    expect(summary2[:files_uploaded]).to eq(0)
  end

  it "never deletes candles files even if delete_after_upload or retain_days is set" do
    candle_dir = File.join(data_dir, "candles", "ibkr", "5min", "2025")
    FileUtils.mkdir_p(candle_dir)
    parquet_file = File.join(candle_dir, "SPX.parquet")
    File.write(parquet_file, "PAR1mockparquetcontent")

    store = publisher.instance_variable_get(:@store)
    allow(store).to receive(:object_exists?).and_return(false)
    allow(store).to receive(:upload_file) do |path, key:|
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: key, size: File.size(path))
    end
    allow(store).to receive(:upload_content).and_return(
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: "k", size: 10)
    )
    allow(publisher).to receive(:count_rows).and_return(10)

    summary = publisher.scan_and_publish(delete_after_upload: true)
    expect(summary[:files_uploaded]).to eq(1)
    expect(summary[:files_deleted]).to eq(0)
    expect(File.exist?(parquet_file)).to be(true)

    ledger_entry = Tickrake::Models::HistoricalFileLedger.find_by(remote_key: "candles/ibkr/5min/2025/SPX.parquet")
    expect(ledger_entry.local_deleted_at).to be_nil
  end

  it "deletes non-candle files immediately when delete_after_upload: true or retain_days: 0" do
    opt_dir = File.join(data_dir, "options", "massive", "2025", "03", "03")
    FileUtils.mkdir_p(opt_dir)
    file1 = File.join(opt_dir, "SPXW_samples_2025-03-03.parquet")
    File.write(file1, "PAR1mockoptionscontent")

    store = publisher.instance_variable_get(:@store)
    allow(store).to receive(:object_exists?).and_return(false)
    allow(store).to receive(:upload_file) do |path, key:|
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: key, size: File.size(path))
    end
    allow(store).to receive(:upload_content).and_return(
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: "k", size: 10)
    )
    allow(publisher).to receive(:count_rows).and_return(20)

    summary = publisher.scan_and_publish(retain_days: 0)
    expect(summary[:files_uploaded]).to eq(1)
    expect(summary[:files_deleted]).to eq(1)
    expect(File.exist?(file1)).to be(false)

    ledger_entry = Tickrake::Models::HistoricalFileLedger.find_by(remote_key: "options/massive/2025/03/03/SPXW_samples_2025-03-03.parquet")
    expect(ledger_entry.local_deleted_at).not_to be_nil
  end

  it "prunes files older than retain_days during retention sweep while preserving candles" do
    opt_dir = File.join(data_dir, "options", "massive", "2025", "03", "01")
    FileUtils.mkdir_p(opt_dir)
    file_old = File.join(opt_dir, "SPXW_samples_2025-03-01.parquet")
    File.write(file_old, "PAR1oldcontent")

    candle_dir = File.join(data_dir, "candles", "ibkr", "5min", "2025")
    FileUtils.mkdir_p(candle_dir)
    candle_file = File.join(candle_dir, "SPX.parquet")
    File.write(candle_file, "PAR1candlecontent")

    store = publisher.instance_variable_get(:@store)
    allow(store).to receive(:object_exists?).and_return(false)
    allow(store).to receive(:upload_file) do |path, key:|
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: key, size: File.size(path))
    end
    allow(store).to receive(:upload_content).and_return(
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-historical", key: "k", size: 10)
    )
    allow(publisher).to receive(:count_rows).and_return(15)

    # First publish without deleting (retain_days: 10)
    publisher.scan_and_publish(retain_days: 10)
    expect(File.exist?(file_old)).to be(true)
    expect(File.exist?(candle_file)).to be(true)

    # Artificially age published_at to 15 days ago in the database
    old_time = (Time.now.utc - (15 * 86_400)).iso8601
    Tickrake::Models::HistoricalFileLedger.where(dataset_type: %w[options candles]).update_all(published_at: old_time)

    # Run sweep with retain_days: 10
    summary = publisher.scan_and_publish(retain_days: 10)
    expect(summary[:files_deleted]).to eq(1)
    expect(File.exist?(file_old)).to be(false)
    expect(File.exist?(candle_file)).to be(true)

    old_record = Tickrake::Models::HistoricalFileLedger.find_by(remote_key: "options/massive/2025/03/01/SPXW_samples_2025-03-01.parquet")
    expect(old_record.local_deleted_at).not_to be_nil

    candle_record = Tickrake::Models::HistoricalFileLedger.find_by(remote_key: "candles/ibkr/5min/2025/SPX.parquet")
    expect(candle_record.local_deleted_at).to be_nil
  end
end
