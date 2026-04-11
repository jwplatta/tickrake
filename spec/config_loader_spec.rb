# frozen_string_literal: true

RSpec.describe Tickrake::ConfigLoader do
  it "loads the example config" do
    config = described_class.load(File.expand_path("../config/tickrake.example.yml", __dir__))

    expect(config.options_monitor_interval_seconds).to eq(300)
    expect(config.dte_buckets).to include(0, 10, 30)
    expect(config.candle_lookback_days).to eq(7)
    expect(config.provider).to eq("schwab")
    expect(config.sqlite_path).to eq(File.expand_path("~/.tickrake/tickrake.sqlite3"))
    expect(config.data_dir).to eq(File.expand_path("~/.tickrake/data"))
    expect(config.history_dir).to eq(File.expand_path("~/.tickrake/data/history"))
    expect(config.options_dir).to eq(File.expand_path("~/.tickrake/data/options"))
    expect(config.options_universe.map(&:symbol)).to include("$SPX", "SPY")
    expect(config.candles_universe.first.frequencies).to include("day", "30min", "10min", "5min", "1min")
  end

  it "preserves explicit legacy storage paths" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "legacy.yml")
      File.write(path, <<~YAML)
        storage:
          data_dir: #{dir}/tickrake-data
          history_dir: #{dir}/legacy-history
          options_dir: #{dir}/legacy-options
        schedule:
          options_monitor:
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
          eod_candles:
            run_at: "16:10"
            days: [mon]
        options:
          universe:
            - symbol: SPY
        candles:
          universe:
            - symbol: SPY
              start_date: "2020-01-01"
              frequencies: [day]
      YAML

      config = described_class.load(path)

      expect(config.data_dir).to eq(File.join(dir, "tickrake-data"))
      expect(config.history_dir).to eq(File.join(dir, "legacy-history"))
      expect(config.options_dir).to eq(File.join(dir, "legacy-options"))
    end
  end

  it "rejects malformed dte buckets" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad.yml")
      File.write(path, <<~YAML)
        schedule:
          options_monitor:
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
          eod_candles:
            run_at: "16:10"
            days: [mon]
        options:
          dte_buckets: [near]
          universe:
            - symbol: SPY
        candles:
          universe:
            - symbol: SPY
              start_date: "2020-01-01"
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Invalid DTE bucket/)
    end
  end

  it "rejects negative candle lookback days" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-lookback.yml")
      File.write(path, <<~YAML)
        schedule:
          options_monitor:
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
          eod_candles:
            run_at: "16:10"
            days: [mon]
        options:
          universe:
            - symbol: SPY
        candles:
          lookback_days: -1
          universe:
            - symbol: SPY
              start_date: "2020-01-01"
              frequencies: [day]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /lookback_days/)
    end
  end
end
