# frozen_string_literal: true

RSpec.describe "job execution" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { Tickrake::Tracker.new(File.join(Dir.mktmpdir, "tickrake.sqlite3")) }
  let(:logger) { Logger.new(nil) }

  def config_with(config, **overrides)
    attrs = config.instance_variables.each_with_object({}) do |ivar, hash|
      hash[ivar.to_s.delete("@").to_sym] = config.instance_variable_get(ivar)
    end
    Tickrake::Config.new(**attrs.merge(overrides))
  end

  it "writes option samples and tracks metadata" do
    Dir.mktmpdir do |dir|
      custom = config_with(config, options_dir: dir)
      client = instance_double("client")
      downloader = Class.new
      stub_const("SchwabRb::OptionSample::Downloader", downloader)
      allow(SchwabRb::OptionSample::Downloader).to receive(:resolve) do |**kwargs|
        path = File.join(
          kwargs.fetch(:directory),
          "SPXW_exp#{kwargs.fetch(:expiration_date).iso8601}_#{kwargs.fetch(:timestamp).strftime("%Y-%m-%d_%H-%M-%S")}.csv"
        )
        File.write(path, "contract_type,symbol\nCALL,SPXW  260409C05100000\n")
        [{ symbol: "$SPX", status: "SUCCESS" }, path]
      end
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)

      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(Dir.glob(File.join(dir, "*.csv"))).not_to be_empty
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(tracker.fetch_runs.map { |row| row["output_path"] }).to all(end_with(".csv"))
      expect(SchwabRb::OptionSample::Downloader).to have_received(:resolve).with(
        hash_including(
          client: client,
          symbol: "$SPX",
          expiration_date: Date.new(2026, 4, 6),
          directory: dir,
          format: "csv",
          root: "SPXW"
        )
      ).at_least(:once)
    end
  end

  it "rejects options collection for non-schwab providers" do
    custom = config_with(config, provider: "ibkr", provider_settings: { "host" => "127.0.0.1" })
    runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, logger: logger)

    expect do
      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))
    end.to raise_error(Tickrake::Error, /provider=schwab only/)
  end

  it "downloads candles through schwab_rb's shared history downloader" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: %w[day 1min 5min],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry])
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger)
      rows = {
        "day" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 21, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)],
        "1min" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 14, 0, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)],
        "5min" => [Tickrake::Data::Bar.new(datetime: Time.utc(2026, 4, 1, 14, 5, 0), open: 1, high: 2, low: 0.5, close: 1.5, volume: 10)]
      }
      allow(provider).to receive(:fetch_bars) do |**kwargs|
        rows.fetch(kwargs.fetch(:frequency))
      end

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(File.exist?(File.join(dir, "schwab", "SPY_day.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab", "SPY_1min.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "schwab", "SPY_5min.csv"))).to eq(true)
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(tracker.fetch_runs.map { |row| row["frequency"] }).to include("day", "1min", "5min")
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(
          symbol: "SPY",
          frequency: "day",
          start_date: Date.new(2020, 1, 1),
          end_date: Date.new(2026, 4, 7)
        )
      )
    end
  end

  it "uses the configured lookback window when a candle file already exists" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "SPY",
        frequencies: ["day"],
        start_date: Date.iso8601("2020-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(config, history_dir: dir, candles_universe: [candle_entry], candle_lookback_days: 3)
      existing_dir = File.join(dir, "schwab")
      FileUtils.mkdir_p(existing_dir)
      existing_path = File.join(existing_dir, "SPY_day.csv")
      File.write(existing_path, "datetime,open,high,low,close,volume\n")
      provider = instance_double(Tickrake::Providers::Schwab, provider_name: "schwab")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger)
      allow(provider).to receive(:fetch_bars).and_return([])

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 4, 3), end_date: Date.new(2026, 4, 7))
      )
    end
  end

  it "uses the configured lookback window for first-time ibkr intraday fetches" do
    candle_entry = Tickrake::CandleSymbol.new(
      symbol: "$SPX",
      frequencies: ["30min"],
      start_date: Date.iso8601("2020-01-01"),
      need_extended_hours_data: false,
      need_previous_close: false
    )
    custom = config_with(
      config,
      provider: "ibkr",
      provider_settings: { "host" => "127.0.0.1" },
      candles_universe: [candle_entry],
      candle_lookback_days: 3
    )
    provider = instance_double(Tickrake::Providers::Ibkr, provider_name: "ibkr")
    provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
    runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger)
    allow(provider).to receive(:fetch_bars).and_return([])

    Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

    expect(provider).to have_received(:fetch_bars).with(
      hash_including(start_date: Date.new(2026, 4, 3), end_date: Date.new(2026, 4, 7), frequency: "30min")
    )
  end

  it "splits ibkr full backfills into smaller candle requests" do
    Dir.mktmpdir do |dir|
      candle_entry = Tickrake::CandleSymbol.new(
        symbol: "$SPX",
        frequencies: ["30min"],
        start_date: Date.iso8601("2026-01-01"),
        need_extended_hours_data: false,
        need_previous_close: false
      )
      custom = config_with(
        config,
        provider: "ibkr",
        provider_settings: { "host" => "127.0.0.1" },
        history_dir: dir,
        candles_universe: [candle_entry]
      )
      provider = instance_double(Tickrake::Providers::Ibkr, provider_name: "ibkr")
      provider_factory = instance_double(Tickrake::ProviderFactory, build: provider)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, provider_factory: provider_factory, logger: logger)
      allow(provider).to receive(:fetch_bars).and_return([])

      Tickrake::CandlesJob.new(runtime, from_config_start: true).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 1, 1), end_date: Date.new(2026, 2, 3), frequency: "30min")
      )
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 2, 4), end_date: Date.new(2026, 3, 9), frequency: "30min")
      )
      expect(provider).to have_received(:fetch_bars).with(
        hash_including(start_date: Date.new(2026, 3, 10), end_date: Date.new(2026, 4, 7), frequency: "30min")
      )
    end
  end
end
