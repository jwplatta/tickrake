# frozen_string_literal: true

RSpec.describe Tickrake::CLI do
  let(:config) do
    instance_double(
      Tickrake::Config,
      jobs: [index_options_job, eod_candles_job],
      job: nil,
      sqlite_path: "/tmp/tickrake.sqlite3",
      database: nil,
      default_provider_name: "schwab"
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
  let(:manual_options_job) do
    Tickrake::ScheduledJobConfig.new(
      name: "manual_options",
      type: "options",
      interval_seconds: nil,
      windows: [],
      run_at: nil,
      days: [],
      lookback_days: nil,
      dte_buckets: [0, 30],
      universe: [double("opt1")],
      manual: true
    )
  end

  before do
    allow(config).to receive(:job).with("index_options").and_return(index_options_job)
    allow(config).to receive(:job).with("eod_candles").and_return(eod_candles_job)
    allow(config).to receive(:job).with("manual_options").and_return(manual_options_job)
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
      allow(Tickrake::PathSupport).to receive(:cli_log_path).and_return(File.join(fake_home, "logs", "cli.log"))

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["init"])

      expect(exit_code).to eq(0)
      expect(File.exist?(File.join(fake_home, "tickrake.yml"))).to eq(true)
      expect(stdout.string).to include("Initialized Tickrake home")
      expect(stdout.string).to include("CLI log file will be written")
    end
  end

  it "runs database migrations only through the dedicated command" do
    stdout = StringIO.new
    stderr = StringIO.new

    allow(Tickrake::Tracker).to receive(:migrate!).with("/tmp/tickrake.sqlite3")

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["migrate"])

    expect(exit_code).to eq(0)
    expect(Tickrake::Tracker).to have_received(:migrate!).with("/tmp/tickrake.sqlite3")
    expect(stdout.string).to include("Migrated Tickrake database")
  end

  it "reports status for configured and orphaned jobs" do
    stdout = StringIO.new
    stderr = StringIO.new
    registry = instance_double(Tickrake::JobRegistry)
    allow(Tickrake::JobRegistry).to receive(:new).and_return(registry)
    allow(registry).to receive(:registered_names).and_return(["orphan_job"])
    allow(registry).to receive(:statuses).with(%w[eod_candles index_options orphan_job]).and_return([
      { name: "eod_candles", state: "stopped" },
      { name: "index_options", state: "running", pid: 123, started_at: "2026-04-10T16:00:00Z", log_path: "/tmp/index_options.log" },
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
      log_path = File.join(dir, "cli.log")
      File.write(log_path, "one\ntwo\nthree\n")
      stdout = StringIO.new
      stderr = StringIO.new

      allow(Tickrake::PathSupport).to receive(:named_log_path).with("cli").and_return(log_path)

      exit_code = described_class.new(stdout: stdout, stderr: stderr).call(["logs", "cli", "--tail", "2"])

      expect(exit_code).to eq(0)
      expect(stdout.string).to eq("two\nthree\n")
    end
  end

  it "executes publish-staging command" do
    stdout = StringIO.new
    stderr = StringIO.new

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(config).to receive(:provider_definition).with("schwab").and_return(double("schwab_provider"))
    publisher = instance_double(Tickrake::Storage::StagingPublisher)
    allow(Tickrake::Storage::StagingPublisher).to receive(:new).and_return(publisher)
    expect(publisher).to receive(:scan_and_publish).with(
      dataset_types: ["options"],
      batch_size: 10,
      delete_after_upload: false,
      after: nil,
      before: nil
    ).and_return(
      files_scanned: 5,
      files_uploaded: 2,
      files_deleted: 0
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "publish-staging",
      "--dataset", "options",
      "--batch-size", "10",
      "--no-delete"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Staging publish complete: scanned 5 file(s), uploaded 2 file(s), deleted 0 file(s).")
  end

  it "executes publish-staging command with --after and --before filters" do
    stdout = StringIO.new
    stderr = StringIO.new

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(config).to receive(:provider_definition).with("schwab").and_return(double("schwab_provider"))
    publisher = instance_double(Tickrake::Storage::StagingPublisher)
    allow(Tickrake::Storage::StagingPublisher).to receive(:new).and_return(publisher)
    expect(publisher).to receive(:scan_and_publish).with(
      dataset_types: ["options"],
      batch_size: nil,
      delete_after_upload: true,
      after: Date.new(2026, 10, 15),
      before: Date.new(2026, 10, 20)
    ).and_return(
      files_scanned: 3,
      files_uploaded: 3,
      files_deleted: 3
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "publish-staging",
      "--dataset", "options",
      "--after", "2026-10-15",
      "--before", "2026-10-20"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Staging publish complete: scanned 3 file(s), uploaded 3 file(s), deleted 3 file(s).")
  end

  it "rejects invalid date format for --after and --before in publish-staging" do
    stdout = StringIO.new
    stderr = StringIO.new

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "publish-staging",
      "--after", "not-a-date"
    ])

    expect(exit_code).to eq(1)
    expect(stderr.string).to include("--after must be a valid ISO8601 date (YYYY-MM-DD) or timestamp.")
  end

  it "rejects --after when greater than or equal to --before in publish-staging" do
    stdout = StringIO.new
    stderr = StringIO.new

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "publish-staging",
      "--after", "2026-10-20",
      "--before", "2026-10-15"
    ])

    expect(exit_code).to eq(1)
    expect(stderr.string).to include("--after must be before --before.")
  end

  it "executes purge-staging command" do
    stdout = StringIO.new
    stderr = StringIO.new

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    allow(config).to receive(:provider_definition).with("schwab").and_return(double("schwab_provider"))
    purger = instance_double(Tickrake::Storage::StagingPurger)
    allow(Tickrake::Storage::StagingPurger).to receive(:new).and_return(purger)
    expect(purger).to receive(:purge).with(
      dataset_types: ["order_book"],
      date: Date.new(2026, 9, 16),
      provider: "schwab",
      dry_run: false,
      purge_remote: true,
      purge_local: false
    ).and_return(
      remote_keys_scanned: 5,
      remote_keys_deleted: 5,
      local_files_scanned: 0,
      local_files_deleted: 0,
      dry_run: false
    )

    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "purge-staging",
      "--date", "2026-09-16",
      "--dataset", "order_book",
      "--provider", "schwab",
      "--remote"
    ])

    expect(exit_code).to eq(0)
    expect(stdout.string).to include("Staging purge complete: remote keys: scanned 5, deleted 5.")
  end

  it "requires --date for purge-staging command" do
    stdout = StringIO.new
    stderr = StringIO.new

    allow(Tickrake::ConfigLoader).to receive(:load).and_return(config)
    exit_code = described_class.new(stdout: stdout, stderr: stderr).call([
      "purge-staging",
      "--dataset", "order_book"
    ])

    expect(exit_code).to eq(1)
    expect(stderr.string).to include("--date YYYY-MM-DD is required for purge-staging.")
  end
end
