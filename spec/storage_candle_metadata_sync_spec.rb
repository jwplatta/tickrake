# frozen_string_literal: true

RSpec.describe Tickrake::Storage::CandleMetadataSync do
  def build_config(history_dir:, options_dir:, sqlite_path:)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: sqlite_path,
      providers: {
        "ibkr-paper" => Tickrake::ProviderDefinition.new(name: "ibkr-paper", adapter: "ibkr", settings: {}, symbol_map: {}),
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: { "/ES" => "^ES" })
      },
      default_provider_name: "ibkr-paper",
      option_root_tickers: {},
      data_dir: File.dirname(history_dir),
      history_dir: history_dir,
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

  it "inserts missing candle metadata rows and skips existing cache paths" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      ibkr_dir = File.join(history_dir, "ibkr-paper")
      FileUtils.mkdir_p(ibkr_dir)

      spy_path = File.join(ibkr_dir, "SPY_1min.csv")
      qqq_path = File.join(ibkr_dir, "QQQ_5min.csv")
      File.write(spy_path, "datetime,open,high,low,close,volume\n2026-04-10T13:30:00Z,1,2,0,1,10\n")
      File.write(qqq_path, "datetime,open,high,low,close,volume\n2026-04-10T13:35:00Z,1,2,0,1,20\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      tracker.upsert_file_metadata(
        path: spy_path,
        dataset_type: "candles",
        provider_name: "ibkr-paper",
        ticker: "SPY",
        frequency: "1min",
        row_count: 9,
        first_observed_at: "2020-01-01T00:00:00Z",
        last_observed_at: "2020-01-01T00:01:00Z",
        file_mtime: 1,
        file_size: 1
      )

      result = described_class.new(config: config, tracker: tracker).run
      rows = tracker.file_metadata_rows(where: "dataset_type = ?", binds: ["candles"], order_by: "path")

      expect(result.providers_scanned).to eq(%w[ibkr-paper schwab])
      expect(result.files_discovered).to eq(2)
      expect(result.rows_inserted).to eq(1)
      expect(result.files_skipped).to eq(1)
      expect(rows.length).to eq(2)
      expect(tracker.file_metadata(spy_path)["row_count"]).to eq(9)
      expect(tracker.file_metadata(qqq_path)["row_count"]).to eq(1)
    end
  end

  it "supports provider filtering and canonicalizes provider symbol mappings" do
    Dir.mktmpdir do |dir|
      history_dir = File.join(dir, "history")
      options_dir = File.join(dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      schwab_dir = File.join(history_dir, "schwab")
      FileUtils.mkdir_p(schwab_dir)

      es_path = File.join(schwab_dir, "^ES_1min.csv")
      File.write(es_path, "datetime,open,high,low,close,volume\n2026-04-10T13:30:00Z,1,2,0,1,10\n")
      File.write(File.join(schwab_dir, "README.txt"), "ignored")
      File.write(File.join(schwab_dir, "badname.csv"), "datetime,open\n")

      config = build_config(history_dir: history_dir, options_dir: options_dir, sqlite_path: sqlite_path)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)

      result = described_class.new(config: config, tracker: tracker, provider_name: "schwab").run
      row = tracker.file_metadata(es_path)

      expect(result.providers_scanned).to eq(["schwab"])
      expect(result.files_discovered).to eq(2)
      expect(result.rows_inserted).to eq(1)
      expect(result.files_skipped).to eq(1)
      expect(row["ticker"]).to eq("^ES")
      expect(row["frequency"]).to eq("1min")
    end
  end
end
