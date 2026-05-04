# frozen_string_literal: true

require "stringio"
require_relative "../scripts/migrate_schwab_option_filenames_to_utc"

RSpec.describe SchwabOptionFilenameUtcMigrator do
  def build_config(dir, timezone: "America/Chicago")
    Tickrake::Config.new(
      timezone: timezone,
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

  it "renames local-time Schwab filenames to UTC filenames and updates metadata timestamps" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_08-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      described_class.new(config: config, stdout: StringIO.new, stderr: StringIO.new).run

      target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_13-30-00.csv")
      metadata = tracker.file_metadata(target_path)
      expect(File.exist?(target_path)).to eq(true)
      expect(File.exist?(source_path)).to eq(false)
      expect(metadata["first_observed_at"]).to eq("2026-04-10T13:30:00Z")
      expect(metadata["last_observed_at"]).to eq("2026-04-10T13:30:00Z")
      expect(tracker.file_metadata(source_path)).to be_nil
    end
  end

  it "inserts inferred metadata when the source file is missing a cache row" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_08-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\nPUT,SPXW\n")

      stdout = StringIO.new
      described_class.new(config: config, stdout: stdout, stderr: StringIO.new).run

      target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_13-30-00.csv")
      metadata = tracker.file_metadata(target_path)
      expect(metadata["provider_name"]).to eq("schwab")
      expect(metadata["ticker"]).to eq("SPXW")
      expect(metadata["expiration_date"]).to eq("2026-04-17")
      expect(metadata["row_count"]).to eq(2)
      expect(metadata["last_observed_at"]).to eq("2026-04-10T13:30:00Z")
      expect(stdout.string).to include("Inserted fresh metadata row")
    end
  end

  it "skips destination collisions without changing source metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-17_2026-04-10_08-30-00.csv")
      target_path = File.join(provider_dir, "2026", "04", "10", "SPXW_exp2026-04-17_2026-04-10_13-30-00.csv")
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
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )
      tracker.upsert_file_metadata(
        path: target_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-17",
        row_count: 1,
        first_observed_at: "2026-04-10T13:30:00Z",
        last_observed_at: "2026-04-10T13:30:00Z",
        file_mtime: 1,
        file_size: 9
      )

      stdout = StringIO.new
      described_class.new(config: config, stdout: stdout, stderr: StringIO.new).run

      expect(File.exist?(source_path)).to eq(true)
      expect(File.exist?(target_path)).to eq(true)
      expect(tracker.file_metadata(source_path)["last_observed_at"]).to eq("2026-04-10T08:30:00Z")
      expect(stdout.string).to include("target already exists")
    end
  end

  it "uses the configured timezone rules for DST-aware conversion" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-12-18_2026-12-10_08-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")

      described_class.new(config: config, stdout: StringIO.new, stderr: StringIO.new).run

      target_path = File.join(config.options_dir, "schwab", "2026", "12", "10", "SPXW_exp2026-12-18_2026-12-10_14-30-00.csv")
      expect(File.exist?(target_path)).to eq(true)
    end
  end
end
