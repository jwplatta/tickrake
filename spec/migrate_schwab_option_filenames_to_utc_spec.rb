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

  it "writes a plan csv in dry-run mode over all Schwab option metadata rows without changing files or metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      plan_path = File.join(dir, "plan.csv")
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      selected_source_path = File.join(provider_dir, "SPXW_exp2026-04-10_2026-04-10_08-30-00.csv")
      other_source_path = File.join(provider_dir, "SPY_exp2026-04-11_2026-04-10_08-31-00.csv")
      File.write(selected_source_path, "contract_type,symbol\nCALL,SPXW\n")
      File.write(other_source_path, "contract_type,symbol\nCALL,SPY\n")
      tracker.upsert_file_metadata(
        path: selected_source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-10",
        row_count: 1,
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )
      tracker.upsert_file_metadata(
        path: other_source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPY",
        frequency: nil,
        expiration_date: "2026-04-11",
        row_count: 1,
        first_observed_at: "2026-04-10T08:31:00Z",
        last_observed_at: "2026-04-10T08:31:00Z",
        file_mtime: 1,
        file_size: 31
      )

      described_class.new(
        config: config,
        plan_csv_path: plan_path,
        apply: false,
        stdout: StringIO.new,
        stderr: StringIO.new
      ).run

      selected_target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-10_2026-04-10_13-30-00.csv")
      other_target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPY_exp2026-04-11_2026-04-10_13-31-00.csv")
      metadata = tracker.file_metadata(selected_source_path)
      expect(File.exist?(selected_target_path)).to eq(false)
      expect(File.exist?(other_target_path)).to eq(false)
      expect(File.exist?(selected_source_path)).to eq(true)
      expect(File.exist?(other_source_path)).to eq(true)
      expect(metadata["first_observed_at"]).to eq("2026-04-10T08:30:00Z")
      expect(metadata["last_observed_at"]).to eq("2026-04-10T08:30:00Z")

      plan_rows = CSV.read(plan_path, headers: true)
      expect(plan_rows.length).to eq(2)
      expect(plan_rows.map { |row| row["ticker"] }).to eq(%w[SPXW SPY])
      expect(plan_rows.map { |row| row["target_path"] }).to eq([selected_target_path, other_target_path])
    end
  end

  it "applies planned metadata updates only when apply is true" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      plan_path = File.join(dir, "plan.csv")
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-10_2026-04-10_08-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\nPUT,SPXW\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-10",
        row_count: 2,
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 42
      )

      stdout = StringIO.new
      described_class.new(
        config: config,
        ticker: "SPXW",
        plan_csv_path: plan_path,
        apply: true,
        stdout: stdout,
        stderr: StringIO.new
      ).run

      target_path = File.join(config.options_dir, "schwab", "2026", "04", "10", "SPXW_exp2026-04-10_2026-04-10_13-30-00.csv")
      metadata = tracker.file_metadata(target_path)
      plan_rows = CSV.read(plan_path, headers: true)

      expect(metadata["provider_name"]).to eq("schwab")
      expect(metadata["ticker"]).to eq("SPXW")
      expect(metadata["expiration_date"]).to eq("2026-04-10")
      expect(metadata["row_count"]).to eq(2)
      expect(metadata["last_observed_at"]).to eq("2026-04-10T13:30:00Z")
      expect(tracker.file_metadata(source_path)).to be_nil
      expect(plan_rows.first["metadata_present"]).to eq("true")
      expect(plan_rows.first["source_last_observed_at"]).to eq("2026-04-10T08:30:00Z")
      expect(plan_rows.first["target_last_observed_at"]).to eq("2026-04-10T13:30:00Z")
      expect(stdout.string).to include("Moved #{source_path}")
    end
  end

  it "writes skip rows to the plan csv for target collisions without changing source metadata" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      plan_path = File.join(dir, "plan.csv")
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-04-10_2026-04-10_08-30-00.csv")
      target_path = File.join(provider_dir, "2026", "04", "10", "SPXW_exp2026-04-10_2026-04-10_13-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")
      FileUtils.mkdir_p(File.dirname(target_path))
      File.write(target_path, "existing\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-10",
        row_count: 1,
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      stdout = StringIO.new
      described_class.new(
        config: config,
        ticker: "SPXW",
        plan_csv_path: plan_path,
        apply: true,
        stdout: stdout,
        stderr: StringIO.new
      ).run

      plan_rows = CSV.read(plan_path, headers: true)
      expect(File.exist?(source_path)).to eq(true)
      expect(File.exist?(target_path)).to eq(true)
      expect(tracker.file_metadata(source_path)["last_observed_at"]).to eq("2026-04-10T08:30:00Z")
      expect(plan_rows.first["action"]).to eq("skip")
      expect(plan_rows.first["reason"]).to eq("target_exists")
      expect(stdout.string).to include("target already exists")
    end
  end

  it "uses configured timezone rules for DST-aware conversion" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      plan_path = File.join(dir, "plan.csv")
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      source_path = File.join(provider_dir, "SPXW_exp2026-12-10_2026-12-10_08-30-00.csv")
      File.write(source_path, "contract_type,symbol\nCALL,SPXW\n")
      tracker.upsert_file_metadata(
        path: source_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-12-10",
        row_count: 1,
        first_observed_at: "2026-12-10T08:30:00Z",
        last_observed_at: "2026-12-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )

      described_class.new(
        config: config,
        ticker: "SPXW",
        plan_csv_path: plan_path,
        apply: true,
        stdout: StringIO.new,
        stderr: StringIO.new
      ).run

      target_path = File.join(config.options_dir, "schwab", "2026", "12", "10", "SPXW_exp2026-12-10_2026-12-10_14-30-00.csv")
      plan_rows = CSV.read(plan_path, headers: true)
      expect(File.exist?(target_path)).to eq(true)
      expect(plan_rows.first["target_path"]).to eq(target_path)
    end
  end

  it "can limit the plan to a single ticker while still scanning all Schwab metadata rows" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      plan_path = File.join(dir, "plan.csv")
      provider_dir = File.join(config.options_dir, "schwab")
      FileUtils.mkdir_p(provider_dir)
      spxw_path = File.join(provider_dir, "SPXW_exp2026-04-10_2026-04-10_08-30-00.csv")
      spy_path = File.join(provider_dir, "SPY_exp2026-04-11_2026-04-10_08-31-00.csv")
      File.write(spxw_path, "contract_type,symbol\nCALL,SPXW\n")
      File.write(spy_path, "contract_type,symbol\nCALL,SPY\n")
      tracker.upsert_file_metadata(
        path: spxw_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-10",
        row_count: 1,
        first_observed_at: "2026-04-10T08:30:00Z",
        last_observed_at: "2026-04-10T08:30:00Z",
        file_mtime: 1,
        file_size: 32
      )
      tracker.upsert_file_metadata(
        path: spy_path,
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPY",
        frequency: nil,
        expiration_date: "2026-04-11",
        row_count: 1,
        first_observed_at: "2026-04-10T08:31:00Z",
        last_observed_at: "2026-04-10T08:31:00Z",
        file_mtime: 1,
        file_size: 31
      )

      described_class.new(
        config: config,
        ticker: "SPXW",
        plan_csv_path: plan_path,
        apply: false,
        stdout: StringIO.new,
        stderr: StringIO.new
      ).run

      plan_rows = CSV.read(plan_path, headers: true)
      expect(plan_rows.length).to eq(1)
      expect(plan_rows.first["ticker"]).to eq("SPXW")
    end
  end
end
