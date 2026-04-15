# frozen_string_literal: true

RSpec.describe Tickrake::Storage::StatsReport do
  it "renders provider counts, sizes, timestamps, and largest files" do
    Dir.mktmpdir do |dir|
      data_dir = File.join(dir, "data")
      history_dir = File.join(data_dir, "history")
      options_dir = File.join(data_dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")
      cli_log_path = File.join(dir, "cli.log")
      options_log_path = File.join(dir, "options.log")
      candles_log_path = File.join(dir, "candles.log")

      FileUtils.mkdir_p(File.join(history_dir, "ibkr-paper"))
      FileUtils.mkdir_p(File.join(options_dir, "schwab"))

      largest_history = File.join(history_dir, "ibkr-paper", "SPY_1min.csv")
      smaller_history = File.join(history_dir, "ibkr-paper", "QQQ_day.csv")
      option_snapshot = File.join(options_dir, "schwab", "SPXW_exp2026-04-11_2026-04-11_10-30-00.csv")

      File.write(largest_history, "1" * 2048)
      File.write(smaller_history, "1" * 512)
      File.write(option_snapshot, "1" * 1024)
      File.write(sqlite_path, "x" * 300)
      File.write(cli_log_path, "x" * 50)
      File.write(options_log_path, "x" * 75)
      File.write(candles_log_path, "x" * 125)

      oldest = Time.utc(2026, 4, 1, 12, 0, 0)
      newest = Time.utc(2026, 4, 12, 15, 30, 0)
      File.utime(oldest, oldest, smaller_history)
      File.utime(newest, newest, largest_history)

      config = Tickrake::Config.new(
        timezone: "America/Chicago",
        sqlite_path: sqlite_path,
        providers: {},
        default_provider_name: "schwab",
        data_dir: data_dir,
        history_dir: history_dir,
        options_dir: options_dir,
        max_workers: 1,
        retry_count: 1,
        retry_delay_seconds: 1,
        option_fetch_timeout_seconds: 30,
        candle_fetch_timeout_seconds: 30,
        options_monitor_interval_seconds: 60,
        options_windows: [],
        eod_run_at: [16, 10],
        eod_days: %w[mon tue wed thu fri],
        candle_lookback_days: 7,
        dte_buckets: [],
        options_universe: [],
        candles_universe: []
      )

      report = described_class.new(
        config,
        log_paths: {
          cli: cli_log_path,
          options: options_log_path,
          candles: candles_log_path
        }
      ).render

      expect(report).to include("Storage stats for #{data_dir}")
      expect(report).to include("Data files: 3 files using 3.5 KB")
      expect(report).to include("Provider folders with data: 2")
      expect(report).to include("History (#{history_dir})")
      expect(report).to include("2 files in 1 provider folders using 2.5 KB")
      expect(report).to include("ibkr-paper: 2 files, 2.5 KB, newest #{newest.getlocal.iso8601}")
      expect(report).to include("oldest file: #{oldest.getlocal.iso8601}")
      expect(report).to include("1. ibkr-paper/SPY_1min.csv (2.0 KB)")
      expect(report).to include("Options (#{options_dir})")
      expect(report).to include("schwab: 1 files, 1.0 KB")
      expect(report).to include("SQLite: 300 B at #{sqlite_path}")
      expect(report).to include("Logs: 250 B across 3 files")
    end
  end

  it "shows missing directories and metadata cleanly" do
    Dir.mktmpdir do |dir|
      data_dir = File.join(dir, "data")
      history_dir = File.join(data_dir, "history")
      options_dir = File.join(data_dir, "options")
      sqlite_path = File.join(dir, "tickrake.sqlite3")

      config = Tickrake::Config.new(
        timezone: "America/Chicago",
        sqlite_path: sqlite_path,
        providers: {},
        default_provider_name: "schwab",
        data_dir: data_dir,
        history_dir: history_dir,
        options_dir: options_dir,
        max_workers: 1,
        retry_count: 1,
        retry_delay_seconds: 1,
        option_fetch_timeout_seconds: 30,
        candle_fetch_timeout_seconds: 30,
        options_monitor_interval_seconds: 60,
        options_windows: [],
        eod_run_at: [16, 10],
        eod_days: %w[mon tue wed thu fri],
        candle_lookback_days: 7,
        dte_buckets: [],
        options_universe: [],
        candles_universe: []
      )

      report = described_class.new(config, log_paths: { cli: File.join(dir, "cli.log") }).render

      expect(report).to include("Data files: 0 files using 0 B")
      expect(report).to include("History (#{history_dir})")
      expect(report).to include("  missing")
      expect(report).to include("Options (#{options_dir})")
      expect(report).to include("SQLite: missing (#{sqlite_path})")
    end
  end
end
