# frozen_string_literal: true

RSpec.describe Tickrake::DeleteCompactedOptionSamples do
  def build_config(dir, with_archive: false)
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

  def write_compaction_fixture(config:, dir:, mismatched_compacted: false, omit_compacted: false)
    sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
    FileUtils.mkdir_p(sample_dir)
    first_raw = File.join(sample_dir, "SPXW_exp2026-06-26_2026-06-26_14-30-00.csv")
    second_raw = File.join(sample_dir, "SPXW_exp2026-06-27_2026-06-26_14-35-00.csv")
    compacted_csv = File.join(sample_dir, "SPXW_samples_2026-06-26.csv")
    compacted_parquet = File.join(sample_dir, "SPXW_samples_2026-06-26.parquet")

    File.write(
      first_raw,
      <<~CSV
        contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
        CALL,SPXW1,desc1,2800.0,2026-06-26,1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
      CSV
    )
    File.write(
      second_raw,
      <<~CSV
        contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
        PUT,SPXW2,desc2,2805.0,2026-06-27,2.1,2.0,4,2.2,5,2.15,1,11,21,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0
      CSV
    )

    unless omit_compacted
      File.write(
        compacted_csv,
        <<~CSV
          contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price,sampled_at
          CALL,SPXW1,desc1,2800.0,2026-06-26,#{mismatched_compacted ? "999.9" : "1.1"},1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0,2026-06-26T14:30:00Z
          PUT,SPXW2,desc2,2805.0,2026-06-27,2.1,2.0,4,2.2,5,2.15,1,11,21,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0,2026-06-26T14:35:00Z
        CSV
      )
    end

    File.write(compacted_parquet, "parquet-placeholder")

    {
      raw_files: [first_raw, second_raw],
      compacted_csv: compacted_csv,
      compacted_parquet: compacted_parquet
    }
  end

  def insert_metadata(tracker, fixture)
    fixture[:raw_files].each do |path|
      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-06-26",
        row_count: 1,
        first_observed_at: "2026-06-26T14:30:00Z",
        last_observed_at: "2026-06-26T14:35:00Z",
        file_mtime: 1,
        file_size: 100
      )
    end

    [
      [fixture[:compacted_csv], "csv"],
      [fixture[:compacted_parquet], "parquet"]
    ].each do |path, format|
      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-06-26",
        storage_format: format,
        storage_location: "local",
        artifact_status: "ready_local",
        remote_uri: nil,
        source_file_count: fixture[:raw_files].length,
        row_count: 2,
        first_observed_at: "2026-06-26T14:30:00Z",
        last_observed_at: "2026-06-26T14:35:00Z",
        file_mtime: 1,
        file_size: 200
      )
    end
  end

  it "returns success in dry-run mode without deleting files or metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir)
      insert_metadata(tracker, fixture)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab",
        dry_run: true
      ).run

      expect(result.safe_to_delete).to eq(true)
      expect(result.dry_run).to eq(true)
      expect(result.deleted_paths).to eq([])
      expect(result.metadata_rows_removed).to eq(nil)
      expect(fixture[:raw_files]).to all(satisfy { |path| File.exist?(path) })
      expect(fixture[:raw_files].map { |path| tracker.file_metadata(path) }).to all(be_a(Hash))
    end
  end

  it "deletes validated raw snapshots and their metadata while keeping compacted artifacts intact" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir)
      insert_metadata(tracker, fixture)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab"
      ).run

      expect(result.safe_to_delete).to eq(true)
      expect(result.deleted_paths).to eq(fixture[:raw_files])
      expect(result.metadata_rows_removed).to eq(2)
      expect(result.deletion_errors).to eq([])
      expect(fixture[:raw_files]).to all(satisfy { |path| !File.exist?(path) })
      expect(tracker.file_metadata(fixture[:raw_files].first)).to eq(nil)
      expect(tracker.file_metadata(fixture[:raw_files].last)).to eq(nil)
      expect(File.exist?(fixture[:compacted_csv])).to eq(true)
      expect(File.exist?(fixture[:compacted_parquet])).to eq(true)
      expect(tracker.file_metadata(fixture[:compacted_csv])["storage_format"]).to eq("csv")
      expect(tracker.file_metadata(fixture[:compacted_parquet])["storage_format"]).to eq("parquet")
    end
  end

  it "leaves files and metadata unchanged when validation fails" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir, mismatched_compacted: true)
      insert_metadata(tracker, fixture)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab"
      ).run

      expect(result.safe_to_delete).to eq(false)
      expect(result.errors).to include("First row mismatch at row 1.")
      expect(result.deleted_paths).to eq([])
      expect(result.metadata_rows_removed).to eq(nil)
      expect(fixture[:raw_files]).to all(satisfy { |path| File.exist?(path) })
      expect(fixture[:raw_files].map { |path| tracker.file_metadata(path) }).to all(be_a(Hash))
    end
  end

  it "leaves files and metadata unchanged when the compacted csv is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir, omit_compacted: true)
      insert_metadata(tracker, fixture)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab"
      ).run

      expect(result.safe_to_delete).to eq(false)
      expect(result.errors.first).to include("Compacted CSV file not found")
      expect(result.deleted_paths).to eq([])
      expect(result.metadata_rows_removed).to eq(nil)
      expect(fixture[:raw_files]).to all(satisfy { |path| File.exist?(path) })
      expect(fixture[:raw_files].map { |path| tracker.file_metadata(path) }).to all(be_a(Hash))
    end
  end

  it "reports partial deletion explicitly and removes metadata only for files that were actually deleted" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir)
      insert_metadata(tracker, fixture)
      failing_path = fixture[:raw_files].last

      allow(File).to receive(:delete).and_call_original
      allow(File).to receive(:delete).with(failing_path).and_raise(Errno::EACCES, failing_path)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab"
      ).run

      expect(result.safe_to_delete).to eq(true)
      expect(result.deleted_paths).to eq([fixture[:raw_files].first])
      expect(result.metadata_rows_removed).to eq(1)
      expect(result.deletion_errors.length).to eq(1)
      expect(result.deletion_errors.first).to include("Failed to delete source snapshot CSV")
      expect(File.exist?(fixture[:raw_files].first)).to eq(false)
      expect(File.exist?(failing_path)).to eq(true)
      expect(tracker.file_metadata(fixture[:raw_files].first)).to eq(nil)
      expect(tracker.file_metadata(failing_path)).not_to eq(nil)
    end
  end

  it "keeps local deletion behavior unchanged when archive config is present but remote metadata is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir, with_archive: true)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_compaction_fixture(config: config, dir: dir)
      insert_metadata(tracker, fixture)

      result = described_class.new(
        config: config,
        tracker: tracker,
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        provider_name: "schwab"
      ).run

      expect(result.safe_to_delete).to eq(true)
      expect(result.deleted_paths).to eq(fixture[:raw_files])
      expect(result.deletion_errors).to eq([])
      expect(result.metadata_rows_removed).to eq(2)
      expect(fixture[:raw_files]).to all(satisfy { |path| !File.exist?(path) })
    end
  end
end
