# frozen_string_literal: true

RSpec.describe "schedulers" do
  let(:config) do
    Tickrake::ConfigLoader.load(File.expand_path("../../config/tickrake.example.yml", __dir__))
  end
  let(:tracker) { instance_double(Tickrake::Tracker) }
  let(:client_factory) { instance_double(Tickrake::ClientFactory) }
  let(:runtime) { Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: Logger.new(nil)) }
  let(:logger) do
    instance_double(Logger, info: nil, warn: nil, error: nil).tap do |double|
      allow(double).to receive(:level=)
    end
  end
  let(:sleeper) do
    double("sleeper").tap do |double|
      allow(double).to receive(:sleep)
    end
  end
  let(:index_options_job) do
    Tickrake::ScheduledJobConfig.new(
      name: "index_options",
      type: "options",
      provider: "schwab",
      interval_seconds: 300,
      windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
      run_at: nil,
      days: [],
      lookback_days: nil,
      dte_buckets: [0, 10, 30],
      universe: [
        Tickrake::OptionSymbol.new(symbol: "$SPX", option_root: "SPXW"),
        Tickrake::OptionSymbol.new(symbol: "SPY", option_root: nil),
        Tickrake::OptionSymbol.new(symbol: "QQQ", option_root: nil)
      ]
    )
  end
  let(:eod_candles_job) do
    Tickrake::ScheduledJobConfig.new(
      name: "eod_candles",
      type: "candles",
      provider: "ibkr-paper",
      interval_seconds: nil,
      windows: [],
      run_at: [16, 5],
      days: %w[mon tue wed thu fri],
      lookback_days: 7,
      dte_buckets: [],
      universe: [
        Tickrake::CandleSymbol.new(symbol: "$SPX", frequencies: %w[day 30min 10min 5min 1min], start_date: Date.iso8601("2020-01-01"))
      ]
    )
  end

  it "runs an options job only inside its configured windows" do
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job)

    inside = Time.new(2026, 4, 6, 9, 0, 0, "-05:00")
    outside = Time.new(2026, 4, 6, 16, 0, 0, "-05:00")

    expect(runner.due?(inside)).to eq(true)
    expect(runner.due?(outside)).to eq(false)
  end

  it "runs a candles job once after configured time" do
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: eod_candles_job)

    before = Time.new(2026, 4, 6, 16, 4, 0, "-05:00")
    after = Time.new(2026, 4, 6, 16, 5, 0, "-05:00")

    expect(runner.due?(before)).to eq(false)
    expect(runner.due?(after)).to eq(true)
  end

  it "runs an interval candle job only inside configured windows and on its interval" do
    job = Tickrake::ScheduledJobConfig.new(
      name: "intraday_candles",
      type: "candles",
      provider: "ibkr-paper",
      interval_seconds: 120,
      windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
      run_at: nil,
      days: [],
      lookback_days: 7,
      dte_buckets: [],
      universe: eod_candles_job.universe
    )
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: job)

    outside = Time.new(2026, 4, 6, 8, 29, 0, "-05:00")
    inside = Time.new(2026, 4, 6, 9, 0, 0, "-05:00")
    not_yet = Time.new(2026, 4, 6, 9, 1, 0, "-05:00")
    due_again = Time.new(2026, 4, 6, 9, 2, 0, "-05:00")

    expect(runner.due?(outside)).to eq(false)
    expect(runner.due?(inside)).to eq(true)

    runner.instance_variable_set(:@last_run_at, inside)

    expect(runner.due?(not_yet)).to eq(false)
    expect(runner.due?(due_again)).to eq(true)
  end

  it "keeps the options scheduler alive after an iteration failure and applies backoff" do
    failing_job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(Timeout::Error, "timed out")

    result = runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))

    expect(result).to eq(true)
    expect(sleeper).to have_received(:sleep).at_least(:once)
    expect(logger).to have_received(:error).with(/iteration failed/)
  end

  it "counts degraded schwab options iterations as failures and exits after the threshold" do
    degraded_job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, degraded_job)
    allow(degraded_job).to receive(:run).and_return(
      Tickrake::ScheduledRunResult.new(success_count: 1, failure_count: 1)
    )

    runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))
    runner.run_iteration(Time.new(2026, 4, 6, 9, 1, 0, "-05:00"))

    expect do
      runner.run_iteration(Time.new(2026, 4, 6, 9, 2, 0, "-05:00"))
    end.to raise_error(Tickrake::SchedulerRestartRequired)

    expect(logger).to have_received(:warn).with(/consecutive_failures=1\/3/)
    expect(logger).to have_received(:warn).with(/consecutive_failures=2\/3/)
    expect(logger).to have_received(:error).with(/reached schwab failure threshold 3\/3/)
  end

  it "counts timeout-degraded schwab options iterations as failures and exits after the threshold" do
    degraded_job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, degraded_job)
    allow(degraded_job).to receive(:run).and_return(
      Tickrake::ScheduledRunResult.new(success_count: 0, failure_count: 1)
    )

    runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))
    runner.run_iteration(Time.new(2026, 4, 6, 9, 1, 0, "-05:00"))

    expect do
      runner.run_iteration(Time.new(2026, 4, 6, 9, 2, 0, "-05:00"))
    end.to raise_error(Tickrake::SchedulerRestartRequired)

    expect(logger).to have_received(:warn).with(/due to degraded run result; consecutive_failures=1\/3/)
    expect(logger).to have_received(:warn).with(/due to degraded run result; consecutive_failures=2\/3/)
    expect(logger).to have_received(:error).with(/reached schwab failure threshold 3\/3/)
  end

  it "counts raised schwab iteration exceptions toward the restart threshold" do
    failing_job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(Timeout::Error, "timed out")

    runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))
    runner.run_iteration(Time.new(2026, 4, 6, 9, 1, 0, "-05:00"))

    expect do
      runner.run_iteration(Time.new(2026, 4, 6, 9, 2, 0, "-05:00"))
    end.to raise_error(Tickrake::SchedulerRestartRequired)

    expect(logger).to have_received(:warn).at_least(:once).with(/due to raised exception/)
  end

  it "resets the consecutive failure count after a successful schwab options iteration" do
    job = instance_double(Tickrake::OptionsJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: index_options_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, job)
    allow(job).to receive(:run).and_return(
      Tickrake::ScheduledRunResult.new(success_count: 1, failure_count: 1),
      Tickrake::ScheduledRunResult.new(success_count: 2, failure_count: 0),
      Tickrake::ScheduledRunResult.new(success_count: 1, failure_count: 1)
    )

    runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))
    runner.run_iteration(Time.new(2026, 4, 6, 9, 1, 0, "-05:00"))
    runner.run_iteration(Time.new(2026, 4, 6, 9, 6, 0, "-05:00"))

    expect(logger).to have_received(:info).with(/reset consecutive failure count/)
    expect(logger).to have_received(:warn).with(/consecutive_failures=1\/3/).twice
  end


  it "keeps the candles scheduler alive after an iteration failure and applies backoff" do
    failing_job = instance_double(Tickrake::CandlesJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: eod_candles_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(StandardError, "boom")

    result = runner.run_iteration(Time.new(2026, 4, 6, 16, 5, 0, "-05:00"))

    expect(result).to eq(true)
    expect(sleeper).to have_received(:sleep).at_least(:once)
    expect(logger).to have_received(:error).with(/iteration failed/)
  end

  it "keeps the interval candle scheduler alive after an iteration failure and applies backoff" do
    failing_job = instance_double(Tickrake::CandlesJob)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, client_factory: client_factory, logger: logger)
    scheduled_job = Tickrake::ScheduledJobConfig.new(
      name: "intraday_candles",
      type: "candles",
      provider: "ibkr-paper",
      interval_seconds: 120,
      windows: [Tickrake::SchedulerWindow.new(days: %w[mon tue wed thu fri], start_time: [8, 30], end_time: [15, 0])],
      run_at: nil,
      days: [],
      lookback_days: 7,
      dte_buckets: [],
      universe: eod_candles_job.universe
    )
    runner = Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: scheduled_job, sleeper: sleeper)

    runner.instance_variable_set(:@job, failing_job)
    allow(failing_job).to receive(:run).and_raise(StandardError, "boom")

    result = runner.run_iteration(Time.new(2026, 4, 6, 9, 0, 0, "-05:00"))

    expect(result).to eq(true)
    expect(sleeper).to have_received(:sleep).at_least(:once)
    expect(logger).to have_received(:error).with(/iteration failed/)
  end

  it "installs signal handlers and executes iterations in StagingPublisherSchedulerRunner" do
    job_double = instance_double(Tickrake::StagingPublisherJob, run: nil)
    scheduled_job = Tickrake::ScheduledJobConfig.new(
      name: "staging_publisher",
      type: "staging_publish",
      interval_seconds: 60,
      windows: []
    )
    runner = Tickrake::StagingPublisherSchedulerRunner.new(runtime, scheduled_job: scheduled_job, sleeper: sleeper)
    runner.instance_variable_set(:@job, job_double)

    expect { runner.send(:install_signal_handlers) }.not_to raise_error
    expect(runner.due?(Time.now)).to eq(true)

    result = runner.run_iteration(Time.now)
    expect(result).to eq(true)
    expect(job_double).to have_received(:run)
  end
end
