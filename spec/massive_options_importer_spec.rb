# frozen_string_literal: true

RSpec.describe Tickrake::Importers::MassiveOptionsImporter do
  def build_config(dir)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "massive" => Tickrake::ProviderDefinition.new(name: "massive", adapter: "massive", settings: {}, symbol_map: {})
      },
      default_provider_name: "massive",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: File.join(dir, "data"),
      history_dir: File.join(dir, "data", "history"),
      options_dir: File.join(dir, "data", "options"),
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 0,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: [
        Tickrake::ScheduledJobConfig.new(
          name: "options",
          type: "options",
          provider: "massive",
          interval_seconds: 300,
          windows: [Tickrake::SchedulerWindow.new(days: %w[mon], start_time: [8, 30], end_time: [15, 0])],
          run_at: nil,
          days: [],
          lookback_days: nil,
          dte_buckets: [0],
          universe: [Tickrake::OptionSymbol.new(symbol: "SPX", option_root: "SPXW")]
        )
      ]
    )
  end

  def write_source(path)
    File.write(path, <<~CSV)
      ticker,volume,open,close,high,low,window_start,transactions
      O:SPXW241202C04300000,4,1739.72,1739.09,1739.72,1739.09,1733115600000000000,4
      O:SPXW241202P04300000,6,1.11,1.22,1.33,1.01,1733115600000000000,2
      O:SPXW241202C04400000,5,1646.6,1645.89,1646.6,1645.89,1733115660000000000,5
      O:SPXW241203C04300000,7,1800.0,1801.0,1802.0,1799.0,1733115600000000000,3
      O:SPX241202C00200000,8,4097.53,4097.53,4097.53,4097.53,1733115600000000000,1
      O:SPY241202C00430000,9,1.0,2.0,3.0,0.5,1733115600000000000,1
    CSV
  end

  it "parses Massive option tickers" do
    parsed = Tickrake::Importers::MassiveOptionSymbol.parse("O:SPXW241202C04300000")

    expect(parsed.massive_root).to eq("SPXW")
    expect(parsed.expiration_date).to eq(Date.new(2024, 12, 2))
    expect(parsed.contract_type).to eq("CALL")
    expect(parsed.strike).to eq(4300.0)
    expect(parsed.symbol).to eq("SPXW241202C04300000")
  end

  it "imports one daily Massive file into separate UTC option snapshot files" do
    Dir.mktmpdir do |dir|
      source_path = File.join(dir, "2024-12-02.csv")
      write_source(source_path)
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)

      results = described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPX",
        option_root: "SPXW",
        source_path: source_path
      ).import

      expect(results.map { |result| File.basename(result.path) }).to contain_exactly(
        "SPXW_exp2024-12-02_2024-12-02_05-00-00.csv",
        "SPXW_exp2024-12-02_2024-12-02_05-01-00.csv",
        "SPXW_exp2024-12-03_2024-12-02_05-00-00.csv"
      )

      first_path = File.join(config.options_dir, "massive", "SPXW_exp2024-12-02_2024-12-02_05-00-00.csv")
      rows = CSV.read(first_path, headers: true)
      expect(rows.length).to eq(2)
      expect(rows.first["contract_type"]).to eq("CALL")
      expect(rows.first["symbol"]).to eq("SPXW241202C04300000")
      expect(rows.first["strike"]).to eq("4300.0")
      expect(rows.first["open"]).to eq("1739.72")
      expect(rows.first["high"]).to eq("1739.72")
      expect(rows.first["low"]).to eq("1739.09")
      expect(rows.first["close"]).to eq("1739.09")
      expect(rows.first["transactions"]).to eq("4")
      expect(rows.first["total_volume"]).to eq("4")
      expect(rows.first["bid"]).to be_nil

      metadata = tracker.file_metadata(first_path)
      expect(metadata["provider_name"]).to eq("massive")
      expect(metadata["ticker"]).to eq("SPXW")
      expect(metadata["expiration_date"]).to eq("2024-12-02")
      expect(metadata["row_count"]).to eq(2)
      expect(metadata["last_observed_at"]).to eq("2024-12-02T05:00:00Z")
    end
  end

  it "batches metadata cache writes once per source import" do
    Dir.mktmpdir do |dir|
      source_path = File.join(dir, "2024-12-02.csv")
      write_source(source_path)
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      allow(tracker).to receive(:bulk_upsert_file_metadata).and_call_original

      described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPX",
        option_root: "SPXW",
        source_path: source_path
      ).import

      expect(tracker).to have_received(:bulk_upsert_file_metadata).once do |rows|
        expect(rows.length).to eq(3)
      end
    end
  end

  it "keeps Massive SPX and SPXW contracts separate by the parsed Massive root token" do
    Dir.mktmpdir do |dir|
      source_path = File.join(dir, "2024-12-02.csv")
      write_source(source_path)
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)

      results = described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPX",
        option_root: "SPX",
        source_path: source_path
      ).import

      expect(results.map { |result| File.basename(result.path) }).to contain_exactly(
        "SPX_exp2024-12-02_2024-12-02_05-00-00.csv"
      )

      rows = CSV.read(results.first.path, headers: true)
      expect(rows.length).to eq(1)
      expect(rows.first["symbol"]).to eq("SPX241202C00200000")
    end
  end

  it "rejects option-root and ticker combinations that contradict shared config" do
    Dir.mktmpdir do |dir|
      source_path = File.join(dir, "2024-12-02.csv")
      write_source(source_path)
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)

      importer = described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPY",
        option_root: "SPXW",
        source_path: source_path
      )

      expect { importer.import }.to raise_error(Tickrake::Error, /maps to ticker SPX/)
    end
  end

  it "fails on target collisions unless force is enabled" do
    Dir.mktmpdir do |dir|
      source_path = File.join(dir, "2024-12-02.csv")
      write_source(source_path)
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      existing_path = File.join(config.options_dir, "massive", "SPXW_exp2024-12-02_2024-12-02_05-00-00.csv")
      FileUtils.mkdir_p(File.dirname(existing_path))
      File.write(existing_path, "old\n")

      importer = described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPX",
        option_root: "SPXW",
        source_path: source_path
      )
      expect { importer.import }.to raise_error(Tickrake::Error, /already exists/)
      expect(File.read(existing_path)).to eq("old\n")

      results = described_class.new(
        config: config,
        tracker: tracker,
        provider_name: "massive",
        ticker: "SPX",
        option_root: "SPXW",
        source_path: source_path,
        force: true
      ).import
      expect(results.length).to eq(3)
      expect(CSV.read(existing_path, headers: true).length).to eq(2)
    end
  end

  it "can read old option sample files with missing Massive columns as nil values" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "old.csv")
      File.write(path, "contract_type,symbol,total_volume\nCALL,SPXW,4\n")

      row = CSV.read(path, headers: true).first

      expect(row["open"]).to be_nil
      expect(row["high"]).to be_nil
      expect(row["low"]).to be_nil
      expect(row["close"]).to be_nil
      expect(row["transactions"]).to be_nil
    end
  end
end
