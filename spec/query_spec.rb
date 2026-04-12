# frozen_string_literal: true

RSpec.describe "query engine" do
  def build_config(history_dir:, options_dir:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(Dir.mktmpdir, "tickrake.sqlite3"),
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}),
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {})
      },
      default_provider_name: "ibkr-paper",
      data_dir: File.dirname(history_dir),
      history_dir: history_dir,
      options_dir: options_dir,
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 1,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      options_monitor_interval_seconds: 300,
      options_windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
      eod_run_at: [16, 10],
      eod_days: %w[mon tue wed thu fri],
      candle_lookback_days: 7,
      dte_buckets: [0, 1],
      options_universe: [
        Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW"),
        Tickrake::OptionSymbol.new(symbol: "SPY", option_root: nil)
      ],
      candles_universe: [
        Tickrake::CandleSymbol.new(
          symbol: "$SPX",
          frequencies: ["30min"],
          start_date: Date.iso8601("2026-01-01"),
          need_extended_hours_data: false,
          need_previous_close: false
        )
      ]
    )
  end

  it "normalizes symbols for canonical comparison and storage tokens" do
    normalizer = Tickrake::Query::SymbolNormalizer.new

    expect(normalizer.canonical("$spx")).to eq("SPX")
    expect(normalizer.storage_token("$spx")).to eq("SPX")
    expect(normalizer.same_symbol?("$SPX", "spx")).to eq(true)
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

  it "groups option snapshot files by canonical ticker through option-root aliases" do
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

      expect(results.length).to eq(1)
      expect(results.first.ticker).to eq("SPX")
      expect(results.first.snapshot_count).to eq(2)
      expect(results.first.first_observed_at).to eq("2026-04-10T14:30:00Z")
      expect(results.first.last_observed_at).to eq("2026-04-11T14:30:00Z")
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

    expect(text).to include("candles provider=ibkr-paper ticker=SPY frequency=1min")
    expect(text).to include("path=/tmp/SPY_1min.csv")
    expect(text).not_to include("646.42")
    expect(JSON.parse(json).fetch("results").first.fetch("path")).to eq("/tmp/SPY_1min.csv")
  end
end
