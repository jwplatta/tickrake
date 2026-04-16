# frozen_string_literal: true

RSpec.describe Tickrake::ConfigLoader do
  it "loads the example config" do
    config = described_class.load(File.expand_path("../config/tickrake.example.yml", __dir__))

    expect(config.options_monitor_interval_seconds).to eq(300)
    expect(config.dte_buckets).to include(0, 10, 30)
    expect(config.candle_lookback_days).to eq(7)
    expect(config.default_provider_name).to eq("schwab")
    expect(config.provider_definition("schwab").adapter).to eq("schwab")
    expect(config.sqlite_path).to eq(File.expand_path("~/.tickrake/tickrake.sqlite3"))
    expect(config.data_dir).to eq(File.expand_path("~/.tickrake/data"))
    expect(config.history_dir).to eq(File.expand_path("~/.tickrake/data/history"))
    expect(config.options_dir).to eq(File.expand_path("~/.tickrake/data/options"))
    expect(config.options_universe.map(&:symbol)).to include("$SPX", "SPY")
    expect(config.candles_universe.first.frequencies).to include("day", "30min", "10min", "5min", "1min")
  end

  it "preserves explicit storage paths" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "storage.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        storage:
          data_dir: #{dir}/tickrake-data
          history_dir: #{dir}/custom-history
          options_dir: #{dir}/custom-options
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
      expect(config.history_dir).to eq(File.join(dir, "custom-history"))
      expect(config.options_dir).to eq(File.join(dir, "custom-options"))
    end
  end

  it "loads named provider settings" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "ibkr.yml")
      File.write(path, <<~YAML)
        default_provider: ib_paper
        providers:
          schwab_main:
            adapter: schwab
          ib_paper:
            adapter: ibkr
            settings:
              host: 10.0.0.5
              port: 7497
              client_id: 77
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

      expect(config.default_provider_name).to eq("ib_paper")
      expect(config.provider_definition("ib_paper").adapter).to eq("ibkr")
      expect(config.provider_definition("ib_paper").settings).to eq(
        "host" => "10.0.0.5",
        "port" => 7497,
        "client_id" => 77
      )
      expect(config.provider_definition("schwab_main").adapter).to eq("schwab")
    end
  end

  it "loads per-provider symbol maps" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "symbols.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
            symbol_map:
              /ES: ^ES
              /NQ: ^NQ
          ibkr-paper:
            adapter: ibkr
            symbol_map:
              ESM6: ^ES
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

      expect(config.provider_definition("schwab").symbol_map).to eq(
        "/ES" => "^ES",
        "/NQ" => "^NQ"
      )
      expect(config.provider_definition("ibkr-paper").symbol_map).to eq(
        "ESM6" => "^ES"
      )
    end
  end

  it "requires providers to be configured" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "missing-providers.yml")
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
          universe:
            - symbol: SPY
              start_date: "2020-01-01"
              frequencies: [day]
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /key not found: "providers"/)
    end
  end

  it "requires default_provider even when only one provider is configured" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "missing-default-single.yml")
      File.write(path, <<~YAML)
        providers:
          schwab:
            adapter: schwab
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

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /key not found: "default_provider"/)
    end
  end

  it "rejects unsupported provider adapters" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-provider.yml")
      File.write(path, <<~YAML)
        default_provider: bad
        providers:
          bad:
            adapter: fake
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

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Unsupported provider adapter/)
    end
  end

  it "rejects malformed provider symbol maps" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad-symbol-map.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
            symbol_map: ["/ES", "^ES"]
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

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /symbol_map must be a mapping/)
    end
  end

  it "requires default_provider when multiple providers are configured" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "missing-default.yml")
      File.write(path, <<~YAML)
        providers:
          schwab_main:
            adapter: schwab
          ib_paper:
            adapter: ibkr
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

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /default_provider/)
    end
  end

  it "rejects malformed dte buckets" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "bad.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
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
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
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
