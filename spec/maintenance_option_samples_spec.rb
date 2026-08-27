# frozen_string_literal: true

RSpec.describe "option sample maintenance" do
  let(:logger) { Logger.new(nil) }

  def build_config(dir, with_archive: true)
    Tickrake::Config.new(
      timezone: "America/Chicago",
      sqlite_path: File.join(dir, "tickrake.sqlite3"),
      providers: {
        "schwab" => Tickrake::ProviderDefinition.new(name: "schwab", adapter: "schwab", settings: {}, symbol_map: {})
      },
      default_provider_name: "schwab",
      option_root_tickers: { "SPXW" => "SPX" },
      option_snapshot_filename_timezone: "utc",
      archives: with_archive ? { "s3_archive" => Tickrake::S3ArchiveConfig.new(bucket: "tickrake", region: "us-east-1", prefix: "", storage_class: "GLACIER_IR") } : {},
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

  def write_raw_fixture(config)
    sample_dir = File.join(config.options_dir, "schwab", "2026", "06", "26")
    FileUtils.mkdir_p(sample_dir)
    raw_a = File.join(sample_dir, "SPXW_exp2026-06-26_2026-06-26_14-30-00.csv")
    raw_b = File.join(sample_dir, "SPXW_exp2026-06-27_2026-06-26_14-35-00.csv")
    File.write(raw_a, <<~CSV)
      contract_type,symbol,description,strike,expiration_date,open,high,low,close,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,transactions,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
      CALL,SPXW1,desc1,2800.0,2026-06-26,1.0,1.2,0.9,1.1,1.1,1.0,2,1.2,3,1.15,1,10,20,4,0.5,0.1,-0.2,0.3,0.05,0.22,0.21,1.05,0.5,0.55,6000.0
    CSV
    File.write(raw_b, <<~CSV)
      contract_type,symbol,description,strike,expiration_date,open,high,low,close,mark,bid,bid_size,ask,ask_size,last,last_size,open_interest,total_volume,transactions,delta,gamma,theta,vega,rho,volatility,theoretical_volatility,theoretical_option_value,intrinsic_value,extrinsic_value,underlying_price
      PUT,SPXW2,desc2,2805.0,2026-06-27,2.0,2.2,1.9,2.1,2.1,2.0,4,2.2,5,2.15,1,11,21,6,-0.5,0.2,-0.3,0.4,-0.05,0.32,0.31,2.05,0.6,1.45,6001.0
    CSV
    { raw_files: [raw_a, raw_b], sample_dir: sample_dir }
  end

  it "runs compaction, validation, archive, source cleanup, and local artifact retention as separate concerns" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      fixture = write_raw_fixture(config)
      progress_reporter = instance_double(Tickrake::ProgressReporter, advance: nil, finish: nil)
      allow(progress_reporter).to receive(:add_total)
      archive_service = instance_double(Tickrake::Storage::S3Archive)
      allow(archive_service).to receive(:upload)
      allow(archive_service).to receive(:verify) do |path|
        key = path.split("/data/").last
        Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake", key: key, size: File.size(path))
      end

      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "SPXW",
        sample_date: Date.new(2026, 6, 26),
        logger: logger
      )

      compact = Tickrake::Maintenance::OptionSamples::Compactor.new(context: context).run(progress_reporter: progress_reporter)
      expect(compact).to be_successful
      expect(compact.artifacts_written.map { |path| File.basename(path) }).to eq(%w[SPXW_samples_2026-06-26.csv SPXW_samples_2026-06-26.parquet])
      compacted_csv_path = compact.artifacts_written.find { |path| path.end_with?(".csv") }
      compacted_csv = CSV.read(compacted_csv_path, headers: true)
      expect(compacted_csv.headers.last).to eq("sampled_at")
      expect(compacted_csv.map { |row| row["sampled_at"] }).to eq(["2026-06-26T14:30:00Z", "2026-06-26T14:35:00Z"])

      validation = Tickrake::Maintenance::OptionSamples::Validator.new(context: context).run
      expect(validation.safe_to_delete).to eq(true)

      archive = Tickrake::Maintenance::OptionSamples::ArtifactArchiver.new(
        context: context,
        archive_services: { "s3_archive" => archive_service }
      ).upload(destination_name: "s3_archive", artifacts: %w[csv parquet])
      expect(archive).to be_successful
      expect(archive.remote_uris.values).to all(include("s3://tickrake/"))

      source_cleanup = Tickrake::Maintenance::OptionSamples::SourceSampleCleaner.new(context: context).run(
        source_paths: validation.source_paths
      )
      expect(source_cleanup).to be_successful
      expect(source_cleanup.deleted_source_paths).to match_array(fixture[:raw_files])

      retention = Tickrake::Maintenance::OptionSamples::LocalArtifactManager.new(context: context).apply(
        remote_uris: archive.remote_uris,
        retain_local: { "csv" => false, "parquet" => true },
        artifacts: %w[csv parquet]
      )
      expect(retention).to be_successful
      expect(retention.retained_local).to eq("csv" => false, "parquet" => true)
    end
  end

  def build_maintenance_job(config, tracker, tasks:)
    Tickrake::MaintenanceJob.new(
      Tickrake::Runtime.new(
        config: config,
        tracker: tracker,
        client_factory: instance_double(Tickrake::ClientFactory),
        logger: logger
      ),
      scheduled_job: Tickrake::ScheduledJobConfig.new(
        name: "compact_spxw",
        type: "maintenance",
        provider: "schwab",
        interval_seconds: nil,
        windows: [],
        run_at: "21:00",
        days: %w[mon tue wed thu fri],
        lookback_days: nil,
        dte_buckets: [],
        universe: [],
        tasks: tasks,
        task: nil,
        settings: {},
        manual: false
      )
    )
  end

  def compact_task(delete_sources: true)
    Tickrake::MaintenanceStepConfig.new(
      action: "compact",
      subject: "option_samples",
      provider: "schwab",
      universe: nil,
      universes: [],
      tickers: [],
      option_root: "SPXW",
      delete_sources: delete_sources,
      destination: nil,
      artifacts: [],
      retain_local: {}
    )
  end

  def archive_task
    Tickrake::MaintenanceStepConfig.new(
      action: "archive",
      subject: "option_samples",
      provider: "schwab",
      universe: nil,
      universes: [],
      tickers: [],
      option_root: "SPXW",
      delete_sources: false,
      destination: "s3_archive",
      artifacts: %w[csv parquet],
      retain_local: { "csv" => false, "parquet" => true }
    )
  end

  def stub_archive_service
    archive_service = instance_double(Tickrake::Storage::S3Archive)
    allow(archive_service).to receive(:upload) do |path|
      key = path.split("/data/").last
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-dev", key: key, size: File.size(path))
    end
    allow(archive_service).to receive(:verify) do |path|
      key = path.split("/data/").last
      Tickrake::Storage::S3Archive::RemoteObject.new(bucket: "tickrake-dev", key: key, size: File.size(path))
    end
    archive_service
  end

  it "publishes ROOT.json and tickers.json after a successful archive step" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      write_raw_fixture(config)

      archive_service = stub_archive_service

      job = build_maintenance_job(config, tracker, tasks: [compact_task, archive_task])

      allow_any_instance_of(Tickrake::Maintenance::OptionSamples::ArtifactArchiver).to receive(:archive_service_for)
        .and_return(archive_service)
      allow_any_instance_of(Tickrake::MaintenanceJob).to receive(:index_s3_archive)
        .and_return(archive_service)

      result = job.run(now: Time.utc(2026, 6, 26, 21, 0, 0))
      expect(result).to be_successful

      root_json = File.join(config.options_dir, "schwab", "SPXW.json")
      tickers_json = File.join(config.options_dir, "schwab", "tickers.json")

      expect(File.exist?(root_json)).to be true
      expect(File.exist?(tickers_json)).to be true

      root_payload = JSON.parse(File.read(root_json))
      expect(root_payload["provider"]).to eq("schwab")
      expect(root_payload["root"]).to eq("SPXW")
      expect(root_payload["historical"].length).to eq(1)
      expect(root_payload["historical"].first["sample_date"]).to eq("2026-06-26")

      tickers_payload = JSON.parse(File.read(tickers_json))
      expect(tickers_payload["roots"]).to include("SPXW")
    end
  end

  it "does not fail the archive step when index publish raises" do
    Dir.mktmpdir do |dir|
      config = build_config(dir)
      Tickrake::Tracker.migrate!(config.sqlite_path)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      write_raw_fixture(config)

      archive_service = stub_archive_service

      job = build_maintenance_job(config, tracker, tasks: [compact_task, archive_task])

      allow_any_instance_of(Tickrake::Maintenance::OptionSamples::ArtifactArchiver).to receive(:archive_service_for)
        .and_return(archive_service)
      allow_any_instance_of(Tickrake::Index::Publisher).to receive(:publish)
        .and_raise(Tickrake::Error, "simulated index publish failure")

      result = job.run(now: Time.utc(2026, 6, 26, 21, 0, 0))
      expect(result).to be_successful
    end
  end

  it "treats a no-source compaction date as a clean skip" do
    Dir.mktmpdir do |dir|
      config = build_config(dir, with_archive: false)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      context = Tickrake::Maintenance::OptionSamples::Context.new(
        config: config,
        tracker: tracker,
        provider_name: "schwab",
        option_root: "DIA",
        sample_date: Date.new(2026, 7, 1),
        logger: logger
      )

      compact = Tickrake::Maintenance::OptionSamples::Compactor.new(context: context).run
      expect(compact).to be_successful
      expect(compact.artifacts_written).to eq([])

      maintenance_job = Tickrake::MaintenanceJob.new(
        Tickrake::Runtime.new(
          config: config,
          tracker: tracker,
          client_factory: instance_double(Tickrake::ClientFactory),
          logger: logger
        ),
        scheduled_job: Tickrake::ScheduledJobConfig.new(
          name: "compact_dia",
          type: "maintenance",
          provider: "schwab",
          interval_seconds: nil,
          windows: [],
          run_at: "15:20",
          days: %w[mon tue wed thu fri],
          lookback_days: nil,
          dte_buckets: [],
          universe: [],
          tasks: [
            Tickrake::MaintenanceStepConfig.new(
              action: "compact",
              subject: "option_samples",
              provider: "schwab",
              universe: nil,
              universes: [],
              tickers: [],
              option_root: "DIA",
              delete_sources: true,
              destination: nil,
              artifacts: [],
              retain_local: {}
            )
          ],
          task: nil,
          settings: {},
          manual: false
        )
      )

      result = maintenance_job.run(now: Time.utc(2026, 7, 1, 21, 0, 0))
      expect(result).to be_successful
      expect(result.artifacts_written).to eq([])
      expect(result.step_results.first.errors).to eq([])
    end
  end
end
