# frozen_string_literal: true

RSpec.describe "schedulers" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { instance_double(Tickrake::Tracker) }
  let(:client_factory) { instance_double(Tickrake::ClientFactory) }
  let(:runtime) { Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: Logger.new(nil)) }

  it "runs options monitor only inside configured windows" do
    runner = Tickrake::OptionsMonitorRunner.new(runtime)

    inside = Time.new(2026, 4, 6, 9, 0, 0, "-05:00")
    outside = Time.new(2026, 4, 6, 16, 0, 0, "-05:00")

    expect(runner.due?(inside)).to eq(true)
    expect(runner.due?(outside)).to eq(false)
  end

  it "runs eod candles once after configured time" do
    runner = Tickrake::EodCandlesRunner.new(runtime)

    before = Time.new(2026, 4, 6, 16, 5, 0, "-05:00")
    after = Time.new(2026, 4, 6, 16, 10, 0, "-05:00")

    expect(runner.due?(before)).to eq(false)
    expect(runner.due?(after)).to eq(true)
  end
end
