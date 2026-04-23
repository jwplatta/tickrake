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

  it "loads multiple named jobs with mixed types and job-level/per-symbol providers" do
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
            provider: ibkr-paper
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
            provider: ibkr-paper
            run_at: "16:05"
            days: [mon]
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [day]
          intraday_candles:
            type: candles
            provider: ibkr-paper
            interval_seconds: 120
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            lookback_days: 7
            universe:
              - symbol: $SPX
                start_date: "2026-03-01"
                frequencies: [30min, 5min, 1min]
      YAML

      config = described_class.load(path)

      expect(config.jobs.map(&:name)).to eq(%w[index_options stock_options eod_candles intraday_candles])
      expect(config.job("index_options").provider).to eq("ibkr-paper")
      expect(config.job("index_options").universe.first.provider).to eq("schwab")
      expect(config.job("stock_options").dte_buckets).to eq([30])
      expect(config.job("eod_candles").run_at).to eq([16, 5])
      expect(config.job("intraday_candles").interval_seconds).to eq(120)
      expect(config.job("intraday_candles").windows.first.days).to eq(["mon"])
      expect(config.provider_name_for_entry(config.job("index_options").universe.first, scheduled_job: config.job("index_options"))).to eq("schwab")
      expect(config.provider_name_for_entry(config.job("stock_options").universe.first, scheduled_job: config.job("stock_options"))).to eq("ibkr-paper")
      expect(config.provider_name_for_entry(config.job("eod_candles").universe.first, scheduled_job: config.job("eod_candles"))).to eq("ibkr-paper")
      expect(config.provider_name_for_entry(config.job("intraday_candles").universe.first, scheduled_job: config.job("intraday_candles"))).to eq("ibkr-paper")
    end
  end

  it "rejects unknown job-level providers" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-job-provider.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          index_options:
            type: options
            provider: missing
            interval_seconds: 300
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            dte_buckets: [0DTE]
            universe:
              - symbol: SPY
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Unknown provider `missing`/)
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

  it "rejects candle jobs that mix daily and interval schedule fields" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "mixed-candle-schedule.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          mixed_candles:
            type: candles
            run_at: "16:05"
            days: [mon]
            interval_seconds: 120
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [day]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /must use either interval_seconds\/windows or run_at\/days, not both/)
    end
  end

  it "rejects candle jobs without a schedule shape" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "missing-candle-schedule.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          missing_candles:
            type: candles
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [day]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /must define either interval_seconds\/windows or run_at\/days/)
    end
  end

  it "rejects non-positive interval candle jobs" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-candle-interval.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          intraday_candles:
            type: candles
            interval_seconds: 0
            windows:
              - days: [mon]
                start: "08:30"
                end: "15:00"
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [1min]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /interval must be positive/)
    end
  end

  it "rejects interval candle jobs without windows" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-candle-windows.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          intraday_candles:
            type: candles
            interval_seconds: 120
            windows: []
            lookback_days: 7
            universe:
              - symbol: SPY
                start_date: "2020-01-01"
                frequencies: [1min]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /At least one candles job window is required/)
    end
  end
end
