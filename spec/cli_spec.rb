# frozen_string_literal: true

RSpec.describe Tickrake::CLI do
  let(:config) do
    instance_double(
      Tickrake::Config,
      jobs: [index_options_job, eod_candles_job],
      job: nil,
      sqlite_path: "/tmp/tickrake.sqlite3"
    )
  end
  let(:index_options_job) do
    Tickrake::ScheduledJobConfig.new(
      name: "index_options",
      type: "options",
      interval_seconds: 300,
      windows: [],
      run_at: nil,
      days: [],
      lookback_days: nil,
      dte_buckets: [0, 1],
      universe: [double("opt1"), double("opt2")]
    )
  end
  let(:eod_candles_job) do
    Tickrake::ScheduledJobConfig.new(
      name: "eod_candles",
      type: "candles",
      interval_seconds: nil,
      windows: [],
      run_at: [16, 5],
      days: %w[mon tue wed thu fri],
      lookback_days: 7,
      dte_buckets: [],
      universe: [double("candle1")]
    )
  end

  before do
    allow(config).to receive(:job).with("index_options").and_return(index_options_job)
    allow(config).to receive(:job).with("eod_candles").and_return(eod_candles_job)
    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
  end

  it "initializes config in Tickrake's own dot folder" do
    Dir.mktmpdir do |dir|
      fake_home = File.join(dir, ".tickrake")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:home_dir).and_return(fake_home)
      allow(Tickrake::PathSupport).to receive(:config_path).and_return(File.join(fake_home, "tickrake.yml"))
      allow(Tickrake::PathSupport).to receive(:sqlite_path).and_return(File.join(fake_home, "tickrake.sqlite3"))
      allow(Tickrake::PathSupport).to receive(:cli_log_path).and_return(File.join(fake_home, "cli.log"))

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["init"])

      expect(exit_code).to eq(0)
      expect(File.exist?(File.join(fake_home, "tickrake.yml"))).to eq(true)
      expect(stdout.string).to include("Initialized Tickrake home")
      expect(stdout.string).to include("CLI log file will be written")
    end
  end

  it "runs a configured options job once with --job" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    progress_reporter = instance_double(Tickrake::ProgressReporter)
    job = instance_double(Tickrake::OptionsJob, run: true)

    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: nil,
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.named_log_path("index_options")
    ).and_return(runtime)
    allow(Tickrake::ProgressReporter).to receive(:build).with(total: 4, title: "Options", output: stdout).and_return(progress_reporter)
    allow(Tickrake::OptionsJob).to receive(:new).with(runtime, progress_reporter: progress_reporter, scheduled_job: index_options_job).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "--job", "index_options"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed job index_options.")
  end

  it "runs a configured candles job once with --job" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: nil,
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.named_log_path("eod_candles")
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(
      runtime,
      from_config_start: true,
      progress_output: stdout,
      scheduled_job: eod_candles_job
    ).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "--job", "eod_candles", "--from-config-start"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed job eod_candles.")
  end

  it "runs direct candles from explicit ticker arguments" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new) do |_, **kwargs|
      universe = kwargs.fetch(:universe)
      expect(kwargs[:from_config_start]).to eq(false)
      expect(kwargs[:start_date_override]).to eq(Date.new(2026, 4, 1))
      expect(kwargs[:end_date_override]).to eq(Date.new(2026, 4, 11))
      expect(kwargs[:progress_output]).to eq(stdout)
      expect(universe.first.symbol).to eq("SPY")
      expect(universe.first.frequencies).to eq(["1min"])
      job
    end

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "run",
      "--type", "candles",
      "--ticker", "SPY",
      "--start-date", "2026-04-01",
      "--end-date", "2026-04-11",
      "--frequency", "minute"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off candle scrape.")
  end

  it "runs direct options from explicit ticker arguments" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::OptionsJob, run: true)

    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::OptionsJob).to receive(:new) do |_, **kwargs|
      universe = kwargs.fetch(:universe)
      expect(kwargs[:expiration_date]).to eq(Date.new(2026, 4, 11))
      expect(kwargs[:progress_reporter]).to be_nil
      expect(universe.first.symbol).to eq("$SPX")
      expect(universe.first.option_root).to eq("SPXW")
      job
    end

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "run",
      "--type", "options",
      "--provider", "schwab",
      "--ticker", "$SPX",
      "--expiration-date", "2026-04-11",
      "--option-root", "SPXW"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off options scrape.")
  end

  it "starts a configured background job by name" do
    stdout = StringIO.new
    stderr = StringIO.new
    controller = instance_double(Tickrake::JobControl, start: true)

    allow(Tickrake::JobControl).to receive(:new).with(stdout: stdout).and_return(controller)
    allow(controller).to receive(:start).with(
      target: "index_options",
      config_path: Tickrake::PathSupport.config_path,
      provider_name: "schwab_live",
      from_config_start: false
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["start", "--job", "index_options", "--provider", "schwab_live"])

    expect(exit_code).to eq(0)
  end

  it "restarts all configured jobs" do
    stdout = StringIO.new
    stderr = StringIO.new
    controller = instance_double(Tickrake::JobControl, restart: true)

    allow(Tickrake::JobControl).to receive(:new).with(stdout: stdout).and_return(controller)
    allow(controller).to receive(:restart).with(
      target: "all",
      config_path: Tickrake::PathSupport.config_path,
      provider_name: nil,
      from_config_start: false
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["restart", "--job", "all"])

    expect(exit_code).to eq(0)
  end

  it "reports status for configured and orphaned jobs" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(registry).to receive(:registered_names).and_return(["orphan_job"])
    allow(registry).to receive(:statuses).with(%w[eod_candles index_options orphan_job]).and_return([
      { name: "index_options", state: "running", pid: 123, started_at: "2026-04-10T16:00:00Z", log_path: "/tmp/index_options.log" },
      { name: "eod_candles", state: "stopped" },
      { name: "orphan_job", state: "stale", pid: 321, started_at: "2026-04-10T15:00:00Z" }
    ])

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["status"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("index_options: running pid=123")
    expect(stdout.string).to include("eod_candles: stopped")
    expect(stdout.string).to include("orphan_job: stale pid=321")
  end

  it "prints the targeted log file tail" do
    Dir.mktmpdir do |dir|
      log_path = File.join(dir, "index_options.log")
      File.write(log_path, "one\ntwo\nthree\n")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:named_log_path).with("index_options").and_return(log_path)

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["logs", "index_options", "--tail", "2"])

      expect(exit_code).to eq(0)
      expect(stdout.string).to eq("two\nthree\n")
    end
  end

  it "rejects removed positional run syntax" do
    stdout = StringIO.new
    stderr = StringIO.new

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "options"])

    expect(exit_code).to eq(1)
    expect(stderr.string).to include("invalid option: options")
  end
end
