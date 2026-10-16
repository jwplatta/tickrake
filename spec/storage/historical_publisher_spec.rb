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
end
