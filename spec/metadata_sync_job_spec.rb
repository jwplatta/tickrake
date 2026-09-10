# frozen_string_literal: true

RSpec.describe Tickrake::MetadataSyncJob do
  let(:logger) { Logger.new(nil) }

  def build_context(dir)
    config = Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: {},
      datastores: {},
      pending_metadata_dir: File.join(dir, "pending_metadata"),
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
    tracker = Tickrake::Tracker.new(config.sqlite_path)
    runtime = Tickrake::Runtime.new(config: config, tracker: tracker, logger: logger)
    scheduled_job = Tickrake::ScheduledJobConfig.new(
      name: "metadata_sync", type: "metadata_sync", provider: nil,
      interval_seconds: 30, windows: [], run_at: nil, days: [], lookback_days: nil,
      dte_buckets: [], universe: [], tasks: [], task: nil,
      settings: { "batch_size" => 500 }, manual: false
    )
    [config, tracker, runtime, scheduled_job]
  end

  def write_sidecar(pending_dir, basename, attrs)
    FileUtils.mkdir_p(pending_dir)
    File.write(File.join(pending_dir, "#{basename}.meta.json"), JSON.generate(attrs))
  end

  it "ingests sidecar files into file_metadata_cache and deletes them" do
    Dir.mktmpdir do |dir|
      config, tracker, runtime, scheduled_job = build_context(dir)
      pending_dir = config.pending_metadata_dir
      now_iso = Time.now.utc.iso8601

      write_sidecar(pending_dir, "SPXW_exp2026-04-06_2026-04-06_14-30-00", {
        "path" => File.join(config.options_dir, "schwab", "2026", "04", "06", "SPXW_exp2026-04-06_2026-04-06_14-30-00.csv"),
        "dataset_type" => "options",
        "provider_name" => "schwab",
        "ticker" => "SPXW",
        "frequency" => nil,
        "expiration_date" => "2026-04-06",
        "row_count" => 42,
        "first_observed_at" => now_iso,
        "last_observed_at" => now_iso,
        "file_mtime" => Time.now.to_i,
        "file_size" => 1024,
        "updated_at" => now_iso,
        "collection_id" => "options-20260406T143000Z"
      })

      expect(Dir.glob(File.join(pending_dir, "*.meta.json")).length).to eq(1)

      described_class.new(runtime, scheduled_job: scheduled_job).run

      expect(Dir.glob(File.join(pending_dir, "*.meta.json"))).to be_empty
      rows = tracker.file_metadata_rows(where: "ticker = 'SPXW'")
      expect(rows.length).to eq(1)
      expect(rows.first["dataset_type"]).to eq("options")
      expect(rows.first["row_count"]).to eq(42)
    end
  end

  it "does nothing when pending_metadata_dir does not exist" do
    Dir.mktmpdir do |dir|
      config, tracker, runtime, scheduled_job = build_context(dir)

      expect { described_class.new(runtime, scheduled_job: scheduled_job).run }.not_to raise_error
      expect(tracker.file_metadata_rows).to be_empty
    end
  end

  it "does nothing when pending_metadata_dir is empty" do
    Dir.mktmpdir do |dir|
      config, tracker, runtime, scheduled_job = build_context(dir)
      FileUtils.mkdir_p(config.pending_metadata_dir)

      described_class.new(runtime, scheduled_job: scheduled_job).run

      expect(tracker.file_metadata_rows).to be_empty
    end
  end

  it "respects batch_size and leaves remaining sidecars for the next run" do
    Dir.mktmpdir do |dir|
      config, tracker, runtime, _ = build_context(dir)
      small_batch_job = Tickrake::ScheduledJobConfig.new(
        name: "metadata_sync", type: "metadata_sync", provider: nil,
        interval_seconds: 30, windows: [], run_at: nil, days: [], lookback_days: nil,
        dte_buckets: [], universe: [], tasks: [], task: nil,
        settings: { "batch_size" => 1 }, manual: false
      )
      pending_dir = config.pending_metadata_dir
      now_iso = Time.now.utc.iso8601
      base_attrs = {
        "dataset_type" => "options", "provider_name" => "schwab", "ticker" => "SPXW",
        "frequency" => nil, "expiration_date" => "2026-04-06", "row_count" => 1,
        "first_observed_at" => now_iso, "last_observed_at" => now_iso,
        "file_mtime" => Time.now.to_i, "file_size" => 512,
        "updated_at" => now_iso, "collection_id" => nil
      }

      write_sidecar(pending_dir, "file_a", base_attrs.merge(
        "path" => File.join(config.options_dir, "file_a.csv")
      ))
      write_sidecar(pending_dir, "file_b", base_attrs.merge(
        "path" => File.join(config.options_dir, "file_b.csv")
      ))

      described_class.new(runtime, scheduled_job: small_batch_job).run

      expect(Dir.glob(File.join(pending_dir, "*.meta.json")).length).to eq(1)
      expect(tracker.file_metadata_rows.length).to eq(1)
    end
  end
end
