# frozen_string_literal: true

RSpec.describe "job execution" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { Tickrake::Tracker.new(File.join(Dir.mktmpdir, "tickrake.sqlite3")) }
  let(:logger) { Logger.new(nil) }
  let(:cli_app) { instance_double(SchwabRb::CLI::App) }

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
      allow(client).to receive(:get_option_expiration_chain).and_return(
        SchwabRb::DataObjects::OptionExpirationChain.build(
          JSON.parse(File.read(File.expand_path("fixtures/option_expiration_chain.json", __dir__)))
        )
      )
      allow(client).to receive(:get_option_chain).and_return(
        JSON.parse(
          File.read(File.expand_path("fixtures/option_chains/ACME_calls.json", __dir__)),
          symbolize_names: true
        )
      )
      allow(SchwabRb::CLI::App).to receive(:new).and_return(cli_app)
      allow(cli_app).to receive(:fetch_option_sample).and_return(
        JSON.parse(
          File.read(File.expand_path("fixtures/option_chains/ACME_calls.json", __dir__)),
          symbolize_names: true
        )
      )
      allow(cli_app).to receive(:option_sample_output_path) do |directory, options, _response|
        File.join(
          directory,
          "#{options[:root] || options[:symbol]}_exp#{options.fetch(:expiration_date).iso8601}_#{options.fetch(:timestamp).strftime("%Y-%m-%d_%H-%M-%S")}.csv"
        )
      end
      allow(cli_app).to receive(:write_option_sample) do |output_path, _response, _options|
        File.write(output_path, "contract_type,symbol\nCALL,SPXW  260409C05100000\n")
      end
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)

      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(Dir.glob(File.join(dir, "*.csv"))).not_to be_empty
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
      expect(cli_app).to have_received(:fetch_option_sample).at_least(:once)
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
