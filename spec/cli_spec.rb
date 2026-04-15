# frozen_string_literal: true

RSpec.describe Tickrake::CLI do
  it "initializes config in Tickrake's own dot folder" do
    Dir.mktmpdir do |dir|
      fake_home = File.join(dir, ".tickrake")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:home_dir).and_return(fake_home)
      allow(Tickrake::PathSupport).to receive(:config_path).and_return(File.join(fake_home, "tickrake.yml"))
      allow(Tickrake::PathSupport).to receive(:sqlite_path).and_return(File.join(fake_home, "tickrake.sqlite3"))
      allow(Tickrake::PathSupport).to receive(:cli_log_path).and_return(File.join(fake_home, "cli.log"))
      allow(Tickrake::PathSupport).to receive(:options_log_path).and_return(File.join(fake_home, "options.log"))
      allow(Tickrake::PathSupport).to receive(:candles_log_path).and_return(File.join(fake_home, "candles.log"))
      allow(Tickrake::PathSupport).to receive(:lock_path) { |name| File.join(fake_home, "#{name}.lock") }

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["init"])

      expect(exit_code).to eq(0)
      expect(File.exist?(File.join(fake_home, "tickrake.yml"))).to eq(true)
      expect(stdout.string).to include("Initialized Tickrake home")
      expect(stdout.string).to include("CLI log file will be written")
      expect(stdout.string).to include("Options job log file will be written")
      expect(stdout.string).to include("Candles job log file will be written")
    end
  end

  it "runs candles once by default" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).with(
      config: anything,
      provider_name: nil,
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.candles_log_path
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(
      runtime,
      from_config_start: false,
      universe: nil,
      start_date_override: nil,
      end_date_override: nil
    ).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off candle scrape.")
  end

  it "runs candles directly from explicit ticker arguments" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: "ib_paper",
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.candles_log_path
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new) do |_, **kwargs|
      universe = kwargs.fetch(:universe)
      expect(kwargs[:from_config_start]).to eq(false)
      expect(kwargs[:start_date_override]).to eq(Date.new(2026, 4, 1))
      expect(kwargs[:end_date_override]).to eq(Date.new(2026, 4, 11))
      expect(universe.length).to eq(1)
      expect(universe.first.symbol).to eq("SPY")
      expect(universe.first.frequencies).to eq(["1min"])
      job
    end

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "run",
      "candles",
      "--provider", "ib_paper",
      "--ticker", "SPY",
      "--start-date", "2026-04-01",
      "--end-date", "2026-04-11",
      "--frequency", "minute"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off candle scrape.")
  end

  it "passes through the from-config-start candle flag" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).with(
      config: anything,
      provider_name: nil,
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.candles_log_path
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(
      runtime,
      from_config_start: true,
      universe: nil,
      start_date_override: nil,
      end_date_override: nil
    ).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles", "--from-config-start"])

    expect(exit_code).to eq(0)
  end

  it "runs options as a long-lived job when requested" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    runner = instance_double(Tickrake::OptionsMonitorRunner, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).with(
      config: anything,
      provider_name: nil,
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.options_log_path
    ).and_return(runtime)
    allow(Tickrake::OptionsMonitorRunner).to receive(:new).with(runtime).and_return(runner)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "options", "--job"])

    expect(exit_code).to eq(0)
  end

  it "runs options directly from explicit ticker arguments" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::OptionsJob, run: true)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: "schwab",
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.options_log_path
    ).and_return(runtime)
    allow(Tickrake::OptionsJob).to receive(:new) do |_, **kwargs|
      universe = kwargs.fetch(:universe)
      expect(kwargs[:expiration_date]).to eq(Date.new(2026, 4, 11))
      expect(universe.length).to eq(1)
      expect(universe.first.symbol).to eq("$SPX")
      expect(universe.first.option_root).to eq("SPXW")
      job
    end

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "run",
      "options",
      "--provider", "schwab",
      "--ticker", "$SPX",
      "--expiration-date", "2026-04-11",
      "--option-root", "SPXW"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off options scrape.")
  end

  it "renders storage stats from configured paths" do
    stdout = StringIO.new
    stderr = StringIO.new
    config = instance_double(
      Tickrake::Config,
      data_dir: "/tmp/data",
      history_dir: "/tmp/data/history",
      options_dir: "/tmp/data/options",
      sqlite_path: "/tmp/tickrake.sqlite3"
    )
    report = instance_double(Tickrake::Storage::StatsReport, render: "stats output")

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Storage::StatsReport).to receive(:new).with(config).and_return(report)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["storage-stats"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("stats output")
  end

  it "starts the options background job" do
    stdout = StringIO.new
    stderr = StringIO.new
    starter = instance_double(Tickrake::BackgroundProcess)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::BackgroundProcess).to receive(:new).with(stdout: stdout).and_return(starter)
    allow(starter).to receive(:start).with(job_name: "options", config_path: Tickrake::PathSupport.config_path, provider_name: nil)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["start", "options"])

    expect(exit_code).to eq(0)
  end

  it "restarts a running job with recorded settings" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    starter = instance_double(Tickrake::BackgroundProcess)

    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(Tickrake::BackgroundProcess).to receive(:new).with(stdout: stdout).and_return(starter)
    allow(registry).to receive(:read).with("candles").and_return(
      config_path: "/tmp/custom.yml",
      provider_name: "ib_paper",
      from_config_start: true
    )
    allow(registry).to receive(:status).with("candles").and_return({ name: "candles", state: "running", pid: 432 })
    allow(registry).to receive(:pid_alive?).with(432).and_return(true, true, false)
    allow(registry).to receive(:delete).with("candles")
    allow(Process).to receive(:kill).with("TERM", 432)
    allow_any_instance_of(Tickrake::CLI).to receive(:sleep)
    allow(starter).to receive(:start).with(
      job_name: "candles",
      config_path: "/tmp/custom.yml",
      from_config_start: true,
      provider_name: "ib_paper"
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["restart", "candles"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Waiting for candles job to finish its current work before restarting. This can take a bit.")
    expect(stdout.string).to include("Stopped candles job")
  end

  it "lets restart flags override recorded job settings" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    starter = instance_double(Tickrake::BackgroundProcess)

    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(Tickrake::BackgroundProcess).to receive(:new).with(stdout: stdout).and_return(starter)
    allow(registry).to receive(:read).with("candles").and_return(
      config_path: "/tmp/custom.yml",
      provider_name: "old_provider",
      from_config_start: false
    )
    allow(registry).to receive(:status).with("candles").and_return({ name: "candles", state: "stopped" })
    allow(starter).to receive(:start).with(
      job_name: "candles",
      config_path: "/tmp/override.yml",
      from_config_start: true,
      provider_name: "new_provider"
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "restart",
      "candles",
      "--provider", "new_provider",
      "--from-config-start",
      "--config", "/tmp/override.yml"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("candles job is not running.")
  end

  it "reports job status" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(registry).to receive(:statuses).and_return([
      { name: "options", state: "running", pid: 123, started_at: "2026-04-10T16:00:00Z", log_path: "/tmp/tickrake.log" },
      { name: "candles", state: "stopped" }
    ])

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["status"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("options: running pid=123")
    expect(stdout.string).to include("candles: stopped")
  end

  it "stops a running job" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(registry).to receive(:status).with("options").and_return({ name: "options", state: "running", pid: 321 })
    allow(registry).to receive(:pid_alive?).with(321).and_return(false)
    allow(registry).to receive(:delete).with("options")
    allow(Process).to receive(:kill).with("TERM", 321)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["stop", "options"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Stopped options job")
  end

  it "prints the targeted log file tail" do
    Dir.mktmpdir do |dir|
      log_path = File.join(dir, "options.log")
      File.write(log_path, "one\ntwo\nthree\n")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:named_log_path).with("options").and_return(log_path)

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["logs", "options", "--tail", "2"])

      expect(exit_code).to eq(0)
      expect(stdout.string).to eq("two\nthree\n")
    end
  end

  it "passes verbose mode into runtime construction" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: nil,
      verbose: true,
      stdout: stdout,
      log_path: Tickrake::PathSupport.candles_log_path
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(
      runtime,
      from_config_start: false,
      universe: nil,
      start_date_override: nil,
      end_date_override: nil
    ).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles", "--verbose"])

    expect(exit_code).to eq(0)
  end

  it "passes the selected provider into runtime for one-off runs" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Runtime).to receive(:new).with(
      config: config,
      provider_name: "ib_paper",
      verbose: false,
      stdout: stdout,
      log_path: Tickrake::PathSupport.candles_log_path
    ).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(
      runtime,
      from_config_start: false,
      universe: nil,
      start_date_override: nil,
      end_date_override: nil
    ).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles", "--provider", "ib_paper"])

    expect(exit_code).to eq(0)
  end

  it "passes the selected provider into background job startup" do
    stdout = StringIO.new
    stderr = StringIO.new
    starter = instance_double(Tickrake::BackgroundProcess)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::BackgroundProcess).to receive(:new).with(stdout: stdout).and_return(starter)
    allow(starter).to receive(:start).with(
      job_name: "candles",
      config_path: Tickrake::PathSupport.config_path,
      from_config_start: false,
      provider_name: "schwab_live"
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["start", "candles", "--provider", "schwab_live"])

    expect(exit_code).to eq(0)
  end

  it "runs query with parsed filters" do
    stdout = StringIO.new
    stderr = StringIO.new
    config = instance_double(Tickrake::Config, sqlite_path: "/tmp/tickrake.sqlite3")
    tracker = instance_double(Tickrake::Tracker)
    engine = instance_double(Tickrake::Query::Engine, run: "ok")

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::Tracker).to receive(:new).with("/tmp/tickrake.sqlite3").and_return(tracker)
    allow(Tickrake::Query::Engine).to receive(:new).with(config: config, tracker: tracker, stdout: stdout).and_return(engine)
    allow(engine).to receive(:run).with(
      type: "candles",
      provider_name: "ibkr-paper",
      ticker: "SPY",
      frequency: "minute",
      start_date: Date.new(2026, 4, 1),
      end_date: Date.new(2026, 4, 11),
      format: "json"
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "query",
      "--type", "candles",
      "--provider", "ibkr-paper",
      "--ticker", "SPY",
      "--frequency", "minute",
      "--start-date", "2026-04-01",
      "--end-date", "2026-04-11",
      "--format", "json"
    ])

    expect(exit_code).to eq(0)
  end
end
