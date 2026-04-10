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
      allow(Tickrake::PathSupport).to receive(:log_path).and_return(File.join(fake_home, "tickrake.log"))
      allow(Tickrake::PathSupport).to receive(:lock_path) { |name| File.join(fake_home, "#{name}.lock") }

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["init"])

      expect(exit_code).to eq(0)
      expect(File.exist?(File.join(fake_home, "tickrake.yml"))).to eq(true)
      expect(stdout.string).to include("Initialized Tickrake home")
      expect(stdout.string).to include("Log file will be written")
    end
  end

  it "runs candles once by default" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(runtime, from_config_start: false).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles"])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Completed one-off candle scrape.")
  end

  it "passes through the from-config-start candle flag" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    job = instance_double(Tickrake::CandlesJob, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(runtime, from_config_start: true).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles", "--from-config-start"])

    expect(exit_code).to eq(0)
  end

  it "runs options as a long-lived job when requested" do
    stdout = StringIO.new
    stderr = StringIO.new
    runtime = instance_double(Tickrake::Runtime)
    runner = instance_double(Tickrake::OptionsMonitorRunner, run: true)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(instance_double(Tickrake::Config))
    allow(Tickrake::Runtime).to receive(:new).and_return(runtime)
    allow(Tickrake::OptionsMonitorRunner).to receive(:new).with(runtime).and_return(runner)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "options", "--job"])

    expect(exit_code).to eq(0)
  end

  it "starts the options background job" do
    stdout = StringIO.new
    stderr = StringIO.new
    starter = instance_double(Tickrake::BackgroundProcess)
    config = instance_double(Tickrake::Config)

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(Tickrake::BackgroundProcess).to receive(:new).with(stdout: stdout).and_return(starter)
    allow(starter).to receive(:start).with(job_name: "options", config_path: Tickrake::PathSupport.config_path)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["start", "options"])

    expect(exit_code).to eq(0)
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

  it "prints the log file tail" do
    Dir.mktmpdir do |dir|
      log_path = File.join(dir, "tickrake.log")
      File.write(log_path, "one\ntwo\nthree\n")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:log_path).and_return(log_path)

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["logs", "--tail", "2"])

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
    allow(Tickrake::Runtime).to receive(:new).with(config: config, verbose: true, stdout: stdout).and_return(runtime)
    allow(Tickrake::CandlesJob).to receive(:new).with(runtime, from_config_start: false).and_return(job)

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["run", "candles", "--verbose"])

    expect(exit_code).to eq(0)
  end
end
