# frozen_string_literal: true

RSpec.describe Tickrake::Storage::CandlesScanner do
  def build_config(candles_dir:, options_dir:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(Dir.mktmpdir, "tickrake.sqlite3"),
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}, symbol_map: {}),
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: { "/ES" => "^ES" })
      },
      default_provider_name: "ibkr-paper",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: File.dirname(candles_dir),
      candles_dir: candles_dir,
      options_dir: options_dir,
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 1,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )
  end

  it "scans candle files and reuses cached metadata on subsequent scans" do
    Dir.mktmpdir do |dir|
      candles_dir = File.join(dir, "candles")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(candles_dir, "ibkr-paper")
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
      config = build_config(candles_dir: candles_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = described_class.new(config: config, tracker: tracker)

      first_results = scanner.scan(provider_name: "ibkr-paper", ticker: "SPY", frequency: "minute")
      cached_results = scanner.scan(provider_name: "ibkr-paper", ticker: "SPY", frequency: "1min")

      expect(first_results.length).to eq(1)
      expect(first_results.first.row_count).to eq(2)
      expect(first_results.first.first_observed_at).to eq("2026-04-10T13:30:00Z")
      expect(cached_results.first.row_count).to eq(2)
    end
  end

  it "applies start and end date filters to candle coverage" do
    Dir.mktmpdir do |dir|
      candles_dir = File.join(dir, "candles")
      options_dir = File.join(dir, "options")
      provider_dir = File.join(candles_dir, "ibkr-paper")
      FileUtils.mkdir_p(provider_dir)
      path = File.join(provider_dir, "SPY_1min.csv")
      File.write(
        path,
        <<~CSV
          datetime,open,high,low,close,volume
          2026-04-10T13:30:00Z,1,2,0.5,1.5,10
          2026-04-12T13:31:00Z,1.5,2,1.4,1.8,12
        CSV
      )
      config = build_config(candles_dir: candles_dir, options_dir: options_dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      scanner = described_class.new(config: config, tracker: tracker)

      full_results = scanner.scan(
        provider_name: "ibkr-paper",
        ticker: "SPY",
        frequency: "1min",
        start_date: Date.new(2026, 4, 10),
        end_date: Date.new(2026, 4, 12)
      )
      none_results = scanner.scan(
        provider_name: "ibkr-paper",
        ticker: "SPY",
        frequency: "1min",
        start_date: Date.new(2026, 4, 13),
        end_date: Date.new(2026, 4, 14)
      )

      expect(full_results.first.coverage).to eq("full")
      expect(none_results).to be_empty
    end
  end
end
