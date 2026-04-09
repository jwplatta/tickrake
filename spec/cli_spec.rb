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
