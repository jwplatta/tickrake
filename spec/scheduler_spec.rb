# frozen_string_literal: true

RSpec.describe "schedulers" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { instance_double(Tickrake::Tracker) }
  let(:client_factory) { instance_double(Tickrake::ClientFactory) }
  let(:runtime) { Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: Logger.new(nil)) }

  it "runs an options job only inside its configured windows" do
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: config.job("index_options"))

    inside = Time.new(2026, 4, 6, 9, 0, 0, "-05:00")
    outside = Time.new(2026, 4, 6, 16, 0, 0, "-05:00")

    expect(runner.due?(inside)).to eq(true)
    expect(runner.due?(outside)).to eq(false)
  end

  it "runs a candles job once after configured time" do
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: config.job("eod_candles"))

    before = Time.new(2026, 4, 6, 16, 4, 0, "-05:00")
    after = Time.new(2026, 4, 6, 16, 5, 0, "-05:00")

    expect(runner.due?(before)).to eq(false)
    expect(runner.due?(after)).to eq(true)
  end

  it "keeps the options scheduler alive after an iteration failure and applies backoff" do
    sleeper = double("sleeper")
    allow(sleeper).to receive(:sleep)
    logger = instance_double(Logger, info: nil, error: nil)
    allow(logger).to receive(:level=)
    failing_job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: config.job("index_options"), sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(Timeout::Error, "timed out")

    result = runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))

    expect(result).to eq(true)
    expect(sleeper).to have_received(:sleep).with(config.retry_delay_seconds)
    expect(logger).to have_received(:error).with(/iteration failed/)
  end

  it "keeps the candles scheduler alive after an iteration failure and applies backoff" do
    sleeper = double("sleeper")
    allow(sleeper).to receive(:sleep)
    logger = instance_double(Logger, info: nil, error: nil)
    allow(logger).to receive(:level=)
    failing_job = instance_double(Tickrake::CandlesJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: config.job("eod_candles"), sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(StandardError, "boom")

    result = runner.run_iteration(Time.new(2026, 4, 6, 16, 5, 0, "-05:00"))

    expect(result).to eq(true)
    expect(sleeper).to have_received(:sleep).with(config.retry_delay_seconds)
    expect(logger).to have_received(:error).with(/iteration failed/)
  end
end
