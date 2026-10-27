# frozen_string_literal: true

RSpec.describe Tickrake::ConfigLoader do
  it "loads the example config" do
    config = described_class.load(File.expand_path("../config/tickrake.example.yml", __dir__))

    expect(config.jobs).to eq([])
    expect(config.default_provider_name).to eq("schwab")
    expect(config.provider_definition("schwab").adapter).to eq("schwab")
    expect(config.provider_definition("schwab").restart_after_consecutive_failures).to eq(3)
    expect(config.provider_definition("schwab").restart_cooldown_seconds).to eq(30)
    expect(config.provider_definition("ibkr-paper").restart_after_consecutive_failures).to be_nil
    expect(config.provider_definition("massive").adapter).to eq("massive")
    expect(config.ticker_for_option_root("SPXW")).to eq("SPX")
    expect(config.option_snapshot_filename_timezone).to eq("utc")
    expect(config.ticker_for_option_root("SPX")).to eq("SPX")
    expect(config.import_job("spxw_massive_options").provider).to eq("massive")
    expect(config.import_job("spxw_massive_options").option_root).to eq("SPXW")
    expect(config.import_job("spxw_massive_options").paths).to include(/2025-10-01\.csv/)
    expect(config.universe("index_option_roots").option_roots).to eq(%w[SPXW SPY QQQ])
    expect(config.staging_target.bucket).to eq("tickrake-staging")
    expect(config.staging_target.type).to eq("s3_compatible")
    expect(config.intraday_target.bucket).to eq("tickrake-intraday")
    expect(config.intraday_target.type).to eq("s3_compatible")
  end

  it "loads staging_target and intraday_target with explicit values and ERB interpolation" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      env_file = File.join(dir, ".env")
      File.write(env_file, <<~ENV)
        TEST_STAGING_BUCKET=custom-staging
        TEST_INTRADAY_BUCKET=custom-intraday
      ENV
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        database:
          adapter: sqlite3
          database: <%= ENV.fetch('CUSTOM_DB_PATH', '/tmp/custom.sqlite3') %>
        staging_target:
          type: s3
          bucket: <%= ENV.fetch('TEST_STAGING_BUCKET') %>
          region: us-east-1
          prefix: staging/daily
          storage_class: glacier
        intraday_target:
          type: s3_compatible
          endpoint: http://127.0.0.1:9000
          bucket: <%= ENV.fetch('TEST_INTRADAY_BUCKET') %>
      YAML

      config = described_class.load(path, env_file: env_file)

      expect(config.database.adapter).to eq("sqlite3")
      expect(config.database.database).to eq("/tmp/custom.sqlite3")
      expect(config.staging_target.bucket).to eq("custom-staging")
      expect(config.staging_target.region).to eq("us-east-1")
      expect(config.staging_target.prefix).to eq("staging/daily")
      expect(config.staging_target.storage_class).to eq("GLACIER")
      expect(config.intraday_target.bucket).to eq("custom-intraday")
      expect(config.intraday_target.endpoint).to eq("http://127.0.0.1:9000")
    end
  end

  it "rejects staging_target without a bucket" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        staging_target:
          type: s3
          bucket:
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /staging_target bucket is required/)
    end
  end

  it "rejects invalid staging_target storage_class values" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        staging_target:
          type: s3
          bucket: tickrake
          storage_class: frozen
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /staging_target has invalid storage_class/)
    end
  end

  it "loads import-only jobs without requiring a scheduled job" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: massive
        providers:
          massive:
            adapter: massive
        options:
          root_tickers:
            SPXW: SPX
        imports:
          spxw_backfill:
            type: options
            provider: massive
            option_root: SPXW
            paths:
              - /tmp/2025-10-01.csv
              - /tmp/2025-10-02.csv
      YAML

      config = described_class.load(path)

      expect(config.jobs).to eq([])
      expect(config.import_job("spxw_backfill").ticker).to be_nil
      expect(config.import_job("spxw_backfill").paths).to eq(["/tmp/2025-10-01.csv", "/tmp/2025-10-02.csv"])
    end
  end

  it "loads explicit provider scheduler resilience settings" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
            settings:
              restart_after_consecutive_failures: 5
              restart_cooldown_seconds: 45
      YAML

      config = described_class.load(path)
      provider = config.provider_definition("schwab")

      expect(provider.restart_after_consecutive_failures).to eq(5)
      expect(provider.restart_cooldown_seconds).to eq(45)
    end
  end

  it "loads an explicit option snapshot filename timezone" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        options:
          snapshot_filename_timezone: America/Chicago
      YAML

      config = described_class.load(path)

      expect(config.option_snapshot_filename_timezone).to eq("America/Chicago")
    end
  end

  it "rejects an invalid option snapshot filename timezone" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        options:
          snapshot_filename_timezone: Mars/Olympus
      YAML

      expect { described_class.load(path) }.to raise_error(Tickrake::ConfigError, /Invalid options\.snapshot_filename_timezone/)
    end
  end

  it "loads shared option root ticker exceptions" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: massive
        providers:
          massive:
            adapter: massive
        options:
          root_tickers:
            SPXW: SPX
      YAML

      config = described_class.load(path)

      expect(config.option_root_tickers).to eq("SPXW" => "SPX")
      expect(config.ticker_for_option_root("SPXW")).to eq("SPX")
      expect(config.ticker_for_option_root("SPX")).to eq("SPX")
    end
  end


  it "loads named universes from an external yaml file" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      universe_path = File.join(dir, "universes.yml")

      File.write(universe_path, <<~YAML)
        universes:
          trading_symbols:
            - symbol: $SPX
              option_roots: [SPXW]
            - symbol: SPY
          futures_symbols:
            - symbol: /ES
      YAML

      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
            symbol_map:
              /ES: ^ES
          ibkr-paper:
            adapter: ibkr
        universes:
          file: universes.yml
        datastores:
          s3_archive:
            type: s3
            bucket: tickrake
      YAML

      config = described_class.load(path)

      expect(config.universe("trading_symbols").symbols).to eq(["$SPX", "SPY"])
      expect(config.universe("trading_symbols").option_roots).to eq(%w[SPXW SPY])
      expect(config.universe("futures_symbols").symbols).to eq(["/ES"])
    end
  end

  it "accepts a massive import-only provider" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: massive
        providers:
          massive:
            adapter: massive
      YAML

      config = described_class.load(path)

      expect(config.provider_definition("massive").adapter).to eq("massive")
    end
  end


  it "rejects schedule defined in YAML configuration" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.yml")
      File.write(path, <<~YAML)
        default_provider: schwab
        providers:
          schwab:
            adapter: schwab
        schedule:
          index_options:
            type: options
      YAML

      expect do
        described_class.load(path)
      end.to raise_error(Tickrake::ConfigError, /Defining jobs in YAML configuration \('schedule'\) is no longer supported/)
    end
  end
end
