# frozen_string_literal: true

RSpec.describe Tickrake::ConfigLoader do
  it "loads the example config with typed scheduled jobs" do
    config = described_class.load(File.expand_path("../config/tickrake.example.yml", __dir__))

    expect(config.jobs.map(&:name)).to eq(%w[index_options eod_candles])
    expect(config.job("index_options").type).to eq("options")
    expect(config.job("index_options").interval_seconds).to eq(300)
    expect(config.job("index_options").dte_buckets).to include(0, 10, 30)
    expect(config.job("eod_candles").type).to eq("candles")
    expect(config.job("eod_candles").lookback_days).to eq(7)
    expect(config.default_provider_name).to eq("schwab")
    expect(config.provider_definition("schwab").adapter).to eq("schwab")
    expect(config.options_universe.map(&:symbol)).to include("$SPX", "SPY")
    expect(config.job("eod_candles").universe.first.frequencies).to include("day", "30min", "10min", "5min", "1min")
  end

  it "loads multiple named jobs with mixed types and per-symbol providers" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: ibkr-paper
        providers:
          schwab:
            adapter: schwab
          ibkr-paper:
            adapter: ibkr
            settings:
              host: 127.0.0.1
        schedule:
          index_options:
            type: options
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            dte_buckets: [0DTE, 2DTE]
            universe:
              - symbol: $SPX
                option_root: SPXW
                provider: schwab
          stock_options:
            type: options
            interval_seconds: 1800
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            dte_buckets: [30DTE]
            universe:
              - symbol: AAPL
          eod_candles:
            type: candles
            run_at: "16:05"
            days: [mon]
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [day]
      YAML

      config = described_class.load(path)

      expect(config.jobs.map(&:name)).to eq(%w[index_options stock_options eod_candles])
      expect(config.job("index_options").universe.first.provider).to eq("schwab")
      expect(config.job("stock_options").dte_buckets).to eq([30])
      expect(config.job("eod_candles").run_at).to eq([16, 5])
      expect(config.provider_name_for_entry(config.job("index_options").universe.first)).to eq("schwab")
      expect(config.provider_name_for_entry(config.job("stock_options").universe.first)).to eq("ibkr-paper")
    end
  end

  it "rejects missing job types" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "missing-type.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          index_options:
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            dte_buckets: [0DTE]
            universe:
              - symbol: SPY
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /must define type/)
    end
  end

  it "rejects unknown job types" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "unknown-type.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          weird_job:
            type: snapshots
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Unknown job type/)
    end
  end

  it "rejects malformed dte buckets" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-buckets.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          index_options:
            type: options
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            dte_buckets: [near]
            universe:
              - symbol: SPY
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Invalid DTE bucket/)
    end
  end

  it "rejects negative candle lookback days" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-lookback.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          eod_candles:
            type: candles
            run_at: "16:05"
            days: [mon]
            lookback_days: -1
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [day]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /lookback_days must be non-negative/)
    end
  end
end
