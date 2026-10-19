# frozen_string_literal: true

RSpec.describe Tickrake::Storage::OptionsScanner do
  def build_config(options_dir:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(Dir.mktmpdir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: options_dir,
      candles_dir: options_dir,
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
          windows: [],
          run_at: nil,
          days: [],
          lookback_days: nil,
          dte_buckets: [0, 1],
          universe: [
            Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW")
          ]
        )
      ]
    )
  end

  it "scans option metadata rows and resolves canonical tickers" do
    Dir.mktmpdir do |dir|
      config = build_config(options_dir: dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      path = File.join(dir, "SPXW_exp2026-04-17_2026-04-10_13-30-00.csv")

      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        expiration_date: "2026-04-17",
        last_observed_at: "2026-04-10T13:30:00Z",
        row_count: 1
      )

      scanner = described_class.new(config: config, tracker: tracker)
      results = scanner.scan(provider_name: "schwab", ticker: "$SPX", expiration_date: Date.new(2026, 4, 17))

      expect(results.length).to eq(1)
      expect(results.first.ticker).to eq("SPX")
      expect(results.first.root_symbol).to eq("SPXW")
      expect(results.first.expiration_date).to eq("2026-04-17")
      expect(results.first.file_path).to eq(path)
    end
  end
end
