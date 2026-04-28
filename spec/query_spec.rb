# frozen_string_literal: true

RSpec.describe "query engine" do
  def build_config(history_dir:, options_dir:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(Dir.mktmpdir, "tickrake.sqlite3"),
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}, symbol_map: {}),
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: { "/ES" => "^ES" })
      },
      default_provider_name: "ibkr-paper",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: File.dirname(history_dir),
      history_dir: history_dir,
      options_dir: options_dir,
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 1,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: [
        Tickrake::ScheduledJobConfig.new(
          name: "options",
          type: "options",
          interval_seconds: 300,
          windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
          run_at: nil,
          days: [],
          lookback_days: nil,
          dte_buckets: [0, 1],
          universe: [
            Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW"),
            Tickrake::OptionSymbol.new(symbol: "SPY", option_root: nil)
          ]
        ),
        Tickrake::ScheduledJobConfig.new(
          name: "candles",
          type: "candles",
          interval_seconds: nil,
          windows: [],
          run_at: [16, 10],
          days: %w[mon tue wed thu fri],
          lookback_days: 7,
          dte_buckets: [],
          universe: [
            Tickrake::CandleSymbol.new(
              symbol: "$SPX",
              frequencies: ["30min"],
              start_date: Date.iso8601("2026-01-01"),
              need_extended_hours_data: false,
              need_previous_close: false
            )
          ]
        )
      ]
    )
  end

  it "normalizes symbols for canonical comparison and storage tokens" do
    normalizer = Tickrake::Query::SymbolNormalizer.new
    provider_definition = Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: { "/ES" => "^ES", "/NQ" => "^NQ", "/RTY" => "^RTY" })

    expect(normalizer.canonical("$spx")).to eq("SPX")
    expect(normalizer.storage_token("$spx")).to eq("SPX")
    expect(normalizer.same_symbol?("$SPX", "spx")).to eq(true)
    expect(normalizer.canonical("/es", provider_definition: provider_definition)).to eq("^ES")
    expect(normalizer.storage_token("/es", provider_definition: provider_definition)).to eq("^ES")
    expect(normalizer.same_symbol?("/ES", "^es", provider_definition: provider_definition)).to eq(true)
  end

  it "scans candle files and reuses cached metadata on subsequent scans" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(history_dir, "ibkr-paper")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPY_1min.csv")
      File.write(
        path,
        <<~CSV
          datetime,open,high,low,close,volume
          2026-04-10T13:30:00Z,1,2,0.5,1.5,10
          2026-04-10T13:31:00Z,1.5,2,1.4,1.8,12
        CSV
      )
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::CandlesScanner.new(config: config, tracker: tracker)

      first_results = scanner.scan(provider_name: "ibkr-paper", ticker: "SPY", frequency: "minute")
      cached_results = scanner.scan(provider_name: "ibkr-paper", ticker: "SPY", frequency: "1min")

      expect(first_results.length).to eq(1)
      expect(first_results.first.row_count).to eq(2)
      expect(first_results.first.first_observed_at).to eq("2026-04-10T13:30:00Z")
      expect(cached_results.first.row_count).to eq(2)
    end
  end

  it "matches mapped futures aliases when scanning candle files" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(history_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "^ES_1min.csv")
      File.write(
        path,
        <<~CSV
          datetime,open,high,low,close,volume
          2026-04-10T13:30:00Z,1,2,0.5,1.5,10
        CSV
      )
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::CandlesScanner.new(config: config, tracker: tracker)

      slash_results = scanner.scan(provider_name: "schwab", ticker: "/ES", frequency: "minute")
      caret_results = scanner.scan(provider_name: "schwab", ticker: "^ES", frequency: "1min")

      expect(slash_results.length).to eq(1)
      expect(caret_results.length).to eq(1)
      expect(slash_results.first.ticker).to eq("^ES")
      expect(caret_results.first.ticker).to eq("^ES")
      expect(slash_results.first.path).to end_with("/^ES_1min.csv")
    end
  end

  it "lists option snapshots with parsed metadata through option-root aliases" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      File.write(File.join(provider_dir, "SPXW_exp2026-04-18_2026-04-11_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker)

      results = scanner.scan(provider_name: "schwab", ticker: "$SPX")

      expect(results.length).to eq(2)
      expect(results.first.ticker).to eq("SPX")
      expect(results.first.root_symbol).to eq("SPXW")
      expect(results.first.expiration_date).to eq("2026-04-17")
      expect(results.first.sample_datetime).to eq("2026-04-10T14:30:00Z")
      expect(results.first.file_path).to include("SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      expect(results.last.expiration_date).to eq("2026-04-18")
    end
  end

  it "allows option searches by option-root alias" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker)

      results = scanner.scan(provider_name: "schwab", ticker: "SPXW")

      expect(results.length).to eq(1)
      expect(results.first.ticker).to eq("SPX")
      expect(results.first.root_symbol).to eq("SPXW")
      expect(results.first.expiration_date).to eq("2026-04-17")
    end
  end

  it "filters option snapshots by sample datetime window and expiration date independently" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPXW_exp2026-04-06_2026-03-30_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      File.write(File.join(provider_dir, "SPXW_exp2026-04-07_2026-03-30_15-00-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      File.write(File.join(provider_dir, "SPXW_exp2026-04-06_2026-03-31_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker)

      results = scanner.scan(
        provider_name: "schwab",
        ticker: "SPXW",
        start_date: Date.new(2026, 3, 30),
        end_date: Date.new(2026, 3, 30),
        expiration_date: Date.new(2026, 4, 6)
      )

      expect(results.length).to eq(1)
      expect(results.first.expiration_date).to eq("2026-04-06")
      expect(results.first.sample_datetime).to eq("2026-03-30T14:30:00Z")
      expect(results.first.file_path).to include("SPXW_exp2026-04-06_2026-03-30_14-30-00.csv")
    end
  end

  it "sorts option snapshots by sample datetime and can limit the returned samples" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "SPXW_exp2026-04-06_2026-03-30_13-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      File.write(File.join(provider_dir, "SPXW_exp2026-04-06_2026-03-30_14-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      File.write(File.join(provider_dir, "SPXW_exp2026-04-06_2026-03-30_15-30-00.csv"), "contract_type,symbol\nCALL,SPXW\n")
      config = build_config(history_dir: history_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker)

      ascending_results = scanner.scan(provider_name: "schwab", ticker: "SPXW", limit: 2)
      descending_results = scanner.scan(provider_name: "schwab", ticker: "SPXW", limit: 2, ascending: false)

      expect(ascending_results.map(&:sample_datetime)).to eq([
        "2026-03-30T13:30:00Z",
        "2026-03-30T14:30:00Z"
      ])
      expect(descending_results.map(&:sample_datetime)).to eq([
        "2026-03-30T15:30:00Z",
        "2026-03-30T14:30:00Z"
      ])
    end
  end

  it "returns option rows backfilled with expiration dates from cached metadata without rediscovery" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(path, "contract_type,symbol\nCALL,SPXW\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir)
      db = SQLite3::Database.new(config.sqlite_path)
      db.execute_batch(
        <<~SQL
          CREATE TABLE file_metadata_cache (
            path TEXT PRIMARY KEY,
            dataset_type TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            ticker TEXT NOT NULL,
            frequency TEXT,
            row_count INTEGER NOT NULL,
            first_observed_at TEXT,
            last_observed_at TEXT,
            file_mtime INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            updated_at TEXT NOT NULL
          );
        SQL
      )
      db.execute(
        <<~SQL,
          INSERT INTO file_metadata_cache (
            path, dataset_type, provider_name, ticker, frequency, row_count,
            first_observed_at, last_observed_at, file_mtime, file_size, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        SQL
        [
          path,
          "options",
          "schwab",
          "SPXW",
          nil,
          1,
          "2026-04-10T14:30:00Z",
          "2026-04-10T14:30:00Z",
          File.stat(path).mtime.to_i,
          File.size(path),
          "2026-04-10T14:30:00Z"
        ]
      )
      db.close

      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = Tickrake::Query::OptionsScanner.new(config: config, tracker: tracker)

      allow(Dir).to receive(:glob).and_raise("unexpected file enumeration")

      results = scanner.scan(provider_name: "schwab", ticker: "$SPX", expiration_date: Date.new(2026, 4, 17))

      expect(results.length).to eq(1)
      expect(results.first.ticker).to eq("SPX")
      expect(results.first.root_symbol).to eq("SPXW")
      expect(results.first.expiration_date).to eq("2026-04-17")
      expect(results.first.file_path).to eq(path)
    end
  end

  it "formats results as deterministic text and json summaries without raw rows" do
    candle_result = Tickrake::Query::CandlesScanner::Result.new(
      dataset_type: "candles",
      provider_name: "ibkr-paper",
      ticker: "SPY",
      frequency: "1min",
      path: "/tmp/SPY_1min.csv",
      row_count: 2,
      first_observed_at: "2026-04-10T13:30:00Z",
      last_observed_at: "2026-04-10T13:31:00Z",
      coverage: "all"
    )

    text = Tickrake::Query::TextFormatter.new.format(
      results: [candle_result],
      filters: { provider: "ibkr-paper", ticker: "SPY", format: "text" }
    )
    json = Tickrake::Query::JsonFormatter.new.format(
      results: [candle_result],
      filters: { provider: "ibkr-paper", ticker: "SPY", format: "json" }
    )

    expect(text).to include("Filters: provider=ibkr-paper ticker=SPY format=text")
    expect(text).to include("Provider: ibkr-paper")
    expect(text).to include("Type: candles")
    expect(text).to include("Ticker: SPY")
    expect(text).to include("- 1min")
    expect(text).to include("  path: /tmp/SPY_1min.csv")
    expect(text).not_to include("646.42")
    expect(JSON.parse(json).fetch("results").first.fetch("path")).to eq("/tmp/SPY_1min.csv")
  end

  it "formats snapshot-level options metadata without coverage" do
    option_result = Tickrake::Query::OptionsScanner::Result.new(
      dataset_type: "options",
      provider_name: "schwab",
      ticker: "SPX",
      root_symbol: "SPXW",
      expiration_date: "2026-04-10",
      sample_datetime: "2026-04-10T20:04:33Z",
      file_path: "/tmp/SPXW_exp2026-04-10_2026-04-10_20-04-33.csv"
    )

    text = Tickrake::Query::TextFormatter.new.format(
      results: [option_result],
      filters: { provider: "schwab", ticker: "$SPX", type: "options", format: "text" }
    )

    expect(text).to include("Provider: schwab")
    expect(text).to include("Type: options")
    expect(text).to include("Ticker: SPX")
    expect(text).to include("- SPXW exp 2026-04-10")
    expect(text).to include("sample_datetime: 2026-04-10T20:04:33Z")
    expect(text).to include("file_path: /tmp/SPXW_exp2026-04-10_2026-04-10_20-04-33.csv")
    expect(text).not_to include("coverage:")
  end
end
