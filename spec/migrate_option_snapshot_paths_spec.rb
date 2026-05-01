# frozen_string_literal: true

require "stringio"
require_relative "../scripts/migrate_option_snapshot_paths"

RSpec.describe OptionSnapshotPathMigrator do
  def build_config(dir)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      data_dir: File.join(dir, "data"),
      history_dir: File.join(dir, "data", "history"),
      options_dir: File.join(dir, "data", "options"),
      max_workers: 2,
      retry_count: 1,
      retry_delay_seconds: 0,
      option_fetch_timeout_seconds: 30,
      candle_fetch_timeout_seconds: 30,
      import_jobs: [],
      jobs: []
    )
  end

  it "moves legacy flat option snapshots into dated folders and updates metadata paths" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T14:30:00Z",
        last_observed_at: "2026-04-10T14:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      described_class.new(config: config, stdout: StringIO.new, stderr: StringIO.new).run

      target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      expect(File.exist?(target_path)).to eq(true)
      expect(File.exist?(source_path)).to eq(false)
      expect(tracker.file_metadata(target_path)).not_to be_nil
      expect(tracker.file_metadata(source_path)).to be_nil
    end
  end

  it "skips files already in the dated layout" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      FileUtils.mkdir_p(File.dirname(target_path))
      File.write(target_path, "contract_type,symbol\nCALL,SPXW\n")
      tracker.upsert_file_metadata(
        path: target_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T14:30:00Z",
        last_observed_at: "2026-04-10T14:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      stdout = StringIO.new
      described_class.new(config: config, stdout: stdout, stderr: StringIO.new).run

      expect(File.exist?(target_path)).to eq(true)
      expect(stdout.string).to include("Skipped 1 files already in the dated layout.")
    end
  end

  it "ignores non-matching files and fails clearly when metadata is missing" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      File.write(File.join(provider_dir, "README.txt"), "ignore\n")
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")

      migrator = described_class.new(config: config, stdout: StringIO.new, stderr: StringIO.new)

      expect { migrator.run }.to raise_error(Tickrake::Error, /Missing metadata row/)
      expect(File.exist?(source_path)).to eq(true)
    end
  end

  it "fails on destination collisions without changing metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      target_path = File.join(provider_dir, "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")
      FileUtils.mkdir_p(File.dirname(target_path))
      File.write(target_path, "existing\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T14:30:00Z",
        last_observed_at: "2026-04-10T14:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      expect do
        described_class.new(config: config, stdout: StringIO.new, stderr: StringIO.new).run
      end.to raise_error(Tickrake::Error, /Target already exists/)

      expect(File.exist?(source_path)).to eq(true)
      expect(tracker.file_metadata(source_path)).not_to be_nil
    end
  end
end
