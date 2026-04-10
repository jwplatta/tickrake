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
      client = instance_double("client")
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)
      rows = {
        "day" => [{ datetime: Time.utc(2026, 4, 1, 21, 0, 0).to_i * 1000 }],
        "1min" => [{ datetime: Time.utc(2026, 4, 1, 14, 0, 0).to_i * 1000 }],
        "5min" => [{ datetime: Time.utc(2026, 4, 1, 14, 5, 0).to_i * 1000 }]
      }
      allow(SchwabRb::PriceHistory::Downloader).to receive(:resolve) do |**kwargs|
        path = File.join(dir, "SPY_#{kwargs.fetch(:frequency)}.csv")
        File.write(path, "datetime,open,high,low,close,volume\n")
        [{ symbol: "SPY", candles: rows.fetch(kwargs.fetch(:frequency)) }, path]
      end

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(File.exist?(File.join(dir, "SPY_day.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "SPY_1min.csv"))).to eq(true)
      expect(File.exist?(File.join(dir, "SPY_5min.csv"))).to eq(true)
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(tracker.fetch_runs.map { |row| row["frequency"] }).to include("day", "1min", "5min")
      expect(SchwabRb::PriceHistory::Downloader).to have_received(:resolve).with(
        hash_including(
          client: client,
          symbol: "SPY",
          start_date: Date.new(2020, 1, 1),
          end_date: Date.new(2026, 4, 7),
          format: "csv"
        )
      ).at_least(:once)
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
      existing_path = File.join(dir, "SPY_day.csv")
      File.write(existing_path, "datetime,open,high,low,close,volume\n")
      client = instance_double("client")
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)
      allow(SchwabRb::PriceHistory::Downloader).to receive(:resolve).and_return([{ symbol: "SPY", candles: [] }, existing_path])

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(SchwabRb::PriceHistory::Downloader).to have_received(:resolve).with(
        hash_including(start_date: Date.new(2026, 4, 3), end_date: Date.new(2026, 4, 7))
      )
    end
  end
end
