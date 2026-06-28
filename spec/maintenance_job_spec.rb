# frozen_string_literal: true

RSpec.describe Tickrake::MaintenanceTasks::CompactOptionSamples do
  let(:logger) { Logger.new(nil) }

  it "compacts raw option snapshot CSVs into csv and parquet artifacts without mutating the source files" do
    Dir.mktmpdir do |dir|
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      sample_dir = File.join(options_dir, "schwab", "2025", "12", "18")
      FileUtils.mkdir_p(sample_dir)

      raw_a = File.join(sample_dir, "SPXW_exp2025-12-18_2025-12-18_19-50-58.csv")
      raw_b = File.join(sample_dir, "SPXW_exp2025-12-19_2025-12-18_20-10-49.csv")
      raw_contents_a = <<~CSV
        contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
        CALL,SPXW1,desc1,2800.0,2025-12-18,1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
      CSV
      raw_contents_b = <<~CSV
        contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
        PUT,SPXW2,desc2,2850.0,2025-12-19,2.1,2.0,4,2.2,5,2.15,1,11,21,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0
      CSV
      File.write(raw_a, raw_contents_a)
      File.write(raw_b, raw_contents_b)

      config = Tickrake::Config.new(
        timezone: "America/Chicago",
        sqlite_path: sqlite_path,
        providers: { "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {}) },
        default_provider_name: "schwab",
        option_root_tickers: { "SPXW" => "SPX" },
        option_snapshot_filename_timezone: "utc",
        data_dir: dir,
        history_dir: File.join(dir, "history"),
        options_dir: options_dir,
        max_workers: 1,
        retry_count: 0,
        retry_delay_seconds: 0,
        option_fetch_timeout_seconds: 30,
        candle_fetch_timeout_seconds: 30,
        jobs: [],
        import_jobs: []
      )
      Tickrake::Tracker.migrate!(sqlite_path)
      tracker = Tickrake::Tracker.new(sqlite_path)
      runtime = Tickrake::Runtime.new(config: config, tracker: tracker, logger: logger)
      scheduled_job = Tickrake::ScheduledJobConfig.new(
        name: "compact_spxw",
        type: "maintenance",
        provider: "schwab",
        interval_seconds: nil,
        windows: [],
        run_at: nil,
        days: [],
        lookback_days: nil,
        dte_buckets: [],
        universe: [],
        task: "compact_option_samples",
        settings: { "option_root" => "SPXW" },
        manual: true
      )

      result = described_class.new(
        runtime: runtime,
        scheduled_job: scheduled_job,
        start_date: Date.new(2025, 12, 18),
        end_date: Date.new(2025, 12, 18)
      ).run(now: Time.utc(2025, 12, 19, 0, 0, 0))

      csv_path = File.join(sample_dir, "SPXW_samples_2025-12-18.csv")
      parquet_path = File.join(sample_dir, "SPXW_samples_2025-12-18.parquet")

      expect(result.processed_dates).to eq([Date.new(2025, 12, 18)])
      expect(result.artifacts_written).to eq([csv_path, parquet_path])
      expect(File.read(raw_a)).to eq(raw_contents_a)
      expect(File.read(raw_b)).to eq(raw_contents_b)
      expect(CSV.read(csv_path, headers: true).headers.last).to eq("sampled_at")
      expect(CSV.read(csv_path, headers: true).map { |row| row["sampled_at"] }).to eq(
        ["2025-12-18T19:50:58Z", "2025-12-18T20:10:49Z"]
      )

      require "parquet"

      parquet_rows = Parquet.each_row(parquet_path).to_a
      expect(parquet_rows.length).to eq(2)
      expect(parquet_rows.map { |row| row["sampled_at"] }).to eq(
        ["2025-12-18T19:50:58Z", "2025-12-18T20:10:49Z"]
      )

      csv_metadata = tracker.file_metadata(csv_path)
      parquet_metadata = tracker.file_metadata(parquet_path)

      expect(csv_metadata["dataset_type"]).to eq("options_compacted_csv")
      expect(csv_metadata["storage_format"]).to eq("csv")
      expect(csv_metadata["storage_location"]).to eq("local")
      expect(csv_metadata["artifact_status"]).to eq("ready_local")
      expect(csv_metadata["source_file_count"]).to eq(2)
      expect(csv_metadata["row_count"]).to eq(2)
      expect(parquet_metadata["dataset_type"]).to eq("options_compacted_parquet")
      expect(parquet_metadata["storage_format"]).to eq("parquet")
      expect(parquet_metadata["source_file_count"]).to eq(2)
    end
  end

  it "advances a progress reporter once per requested date" do
    Dir.mktmpdir do |dir|
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      %w[2025-12-18 2025-12-19].each do |sample_date|
        sample_dir = File.join(options_dir, "schwab", *sample_date.split("-"))
        FileUtils.mkdir_p(sample_dir)
        File.write(
          File.join(sample_dir, "SPXW_exp#{sample_date}_#{sample_date}_19-50-58.csv"),
          <<~CSV
            contract_type,symbol,description,strike,expiration_date,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
            CALL,SPXW1,desc1,2800.0,#{sample_date},1.1,1.0,2,1.2,3,1.15,1,10,20,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
          CSV
        )
      end

      config = Tickrake::Config.new(
        timezone: "America/Chicago",
        sqlite_path: sqlite_path,
        providers: { "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {}) },
        default_provider_name: "schwab",
        option_root_tickers: { "SPXW" => "SPX" },
        option_snapshot_filename_timezone: "utc",
        data_dir: dir,
        history_dir: File.join(dir, "history"),
        options_dir: options_dir,
        max_workers: 1,
        retry_count: 0,
        retry_delay_seconds: 0,
        option_fetch_timeout_seconds: 30,
        candle_fetch_timeout_seconds: 30,
        jobs: [],
        import_jobs: []
      )
      Tickrake::Tracker.migrate!(sqlite_path)
      tracker = Tickrake::Tracker.new(sqlite_path)
      runtime = Tickrake::Runtime.new(config: config, tracker: tracker, logger: logger)
      scheduled_job = Tickrake::ScheduledJobConfig.new(
        name: "compact_spxw",
        type: "maintenance",
        provider: "schwab",
        interval_seconds: nil,
        windows: [],
        run_at: nil,
        days: [],
        lookback_days: nil,
        dte_buckets: [],
        universe: [],
        task: "compact_option_samples",
        settings: { "option_root" => "SPXW" },
        manual: true
      )
      progress_reporter = instance_double(Tickrake::ProgressReporter, advance: nil, finish: nil)

      described_class.new(
        runtime: runtime,
        scheduled_job: scheduled_job,
        start_date: Date.new(2025, 12, 18),
        end_date: Date.new(2025, 12, 19),
        progress_reporter: progress_reporter
      ).run(now: Time.utc(2025, 12, 19, 0, 0, 0))

      expect(progress_reporter).to have_received(:advance).with(title: "Compact 2025-12-18").ordered
      expect(progress_reporter).to have_received(:advance).with(title: "Compact 2025-12-19").ordered
      expect(progress_reporter).to have_received(:finish).once
    end
  end
end
