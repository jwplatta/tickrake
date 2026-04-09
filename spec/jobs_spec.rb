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
      allow(client).to receive(:get_option_expiration_chain).and_return(
        SchwabRb::DataObjects::OptionExpirationChain.build(
          JSON.parse(File.read("/Users/jplatta/repos/schwab_rb/spec/fixtures/option_expiration_chain.json"))
        )
      )
      allow(client).to receive(:get_option_chain).and_return(
        JSON.parse(File.read("/Users/jplatta/repos/schwab_rb/spec/fixtures/option_chains/ACME_calls.json"), symbolize_names: true)
      )
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)

      Tickrake::OptionsJob.new(runtime).run(now: Time.utc(2026, 4, 6, 14, 30, 0))

      expect(Dir.glob(File.join(dir, "*.csv"))).not_to be_empty
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
    end
  end

  it "merges candle history into the default history naming pattern" do
    Dir.mktmpdir do |dir|
      custom = config_with(config, history_dir: dir)
      client = instance_double("client")
      allow(client).to receive(:get_price_history).and_return(
        {
          symbol: "SPY",
          candles: [
            { datetime: Time.utc(2026, 4, 1, 21, 0, 0).to_i * 1000, open: 1.0, high: 2.0, low: 0.5, close: 1.5, volume: 10 }
          ]
        }
      )
      client_factory = instance_double(Tickrake::ClientFactory, build: client)
      runtime = Tickrake::Runtime.new(config: custom, tracker: tracker, client_factory: client_factory, logger: logger)

      Tickrake::CandlesJob.new(runtime).run(now: Time.utc(2026, 4, 6, 21, 10, 0))

      expect(Dir.glob(File.join(dir, "*.csv"))).not_to be_empty
      expect(tracker.fetch_runs.map { |row| row["status"] }).to all(eq("success"))
    end
  end
end
