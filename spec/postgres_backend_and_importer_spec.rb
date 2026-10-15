# frozen_string_literal: true

require "sqlite3"
require_relative "../scripts/import_sqlite_to_postgres"

RSpec.describe "PostgreSQL backend and SQLite to PostgreSQL importer" do
  let(:pg_db_name) { "tickrake_test_#{Time.now.to_i}_#{rand(1000..9999)}" }
  let(:pg_config) do
    Tickrake::DatabaseConfig.new(
      backend: "postgres",
      database: pg_db_name,
      host: "localhost"
    )
  end

  before(:all) do
    # Verify local PostgreSQL is available
    begin
      conn = PG.connect(dbname: "postgres")
      conn.close
      @pg_available = true
    rescue StandardError => e
      @pg_available = false
      warn "Local PostgreSQL not available, skipping Postgres specs: #{e.message}"
    end
  end

  around(:each) do |example|
    if @pg_available
      admin_conn = PG.connect(dbname: "postgres")
      admin_conn.exec("CREATE DATABASE #{pg_db_name}")
      admin_conn.close

      begin
        example.run
      ensure
        admin_conn = PG.connect(dbname: "postgres")
        admin_conn.exec("DROP DATABASE IF EXISTS #{pg_db_name}")
        admin_conn.close
      end
    else
      skip "Local PostgreSQL not running"
    end
  end

  describe "PostgresAdapter and Tracker migrations" do
    it "runs migrations and verifies tables exist" do
      adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
      expect(adapter.tables).not_to include("fetch_runs")

      Tickrake::Tracker.migrate!(pg_config)

      expect(adapter.tables).to include("fetch_runs", "file_metadata_cache", "api_rate_limits", "tickers")
      adapter.close
    end

    it "records fetch runs and file metadata using Tracker with Postgres backend" do
      Tickrake::Tracker.migrate!(pg_config)
      tracker = Tickrake::Tracker.new(pg_config)

      id = tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: "SPY",
        option_root: "SPY",
        requested_buckets: [0],
        resolved_expiration: "2026-10-15",
        scheduled_for: Time.utc(2026, 10, 15, 14, 30, 0),
        started_at: Time.utc(2026, 10, 15, 14, 30, 1)
      )

      expect(id).to be > 0

      tracker.record_finish(
        id: id,
        status: "success",
        finished_at: Time.utc(2026, 10, 15, 14, 30, 2),
        output_path: "/tmp/spy.csv"
      )

      runs = tracker.fetch_runs
      expect(runs.length).to eq(1)
      expect(runs.first["status"]).to eq("success")
      expect(runs.first["symbol"]).to eq("SPY")

      tracker.upsert_file_metadata(
        path: "/tmp/spy.csv",
        dataset_type: "options",
        provider_name: "schwab",
        ticker: "SPY",
        frequency: nil,
        expiration_date: "2026-10-15",
        row_count: 50,
        first_observed_at: "2026-10-15T14:30:00Z",
        last_observed_at: "2026-10-15T14:30:00Z",
        file_mtime: 12345,
        file_size: 1024
      )

      meta = tracker.file_metadata("/tmp/spy.csv")
      expect(meta["row_count"].to_i).to eq(50)
      expect(meta["ticker"]).to eq("SPY")

      tracker.close
    end

    it "ingests fundamentals sidecars with symbol 'none' and legacy null-symbol sidecars without NOT NULL violation" do
      Tickrake::Tracker.migrate!(pg_config)
      tracker = Tickrake::Tracker.new(pg_config)

      Dir.mktmpdir do |dir|
        pending_dir = File.join(dir, "pending_metadata")
        FileUtils.mkdir_p(pending_dir)

        sidecar_new = {
          "fetch_run" => {
            "job_type" => "daily_fundamentals",
            "dataset_type" => "fundamentals",
            "symbol" => "none",
            "frequency" => nil,
            "option_root" => nil,
            "requested_buckets" => nil,
            "resolved_expiration" => nil,
            "scheduled_for" => "2026-10-15T15:30:00Z",
            "started_at" => "2026-10-15T15:30:01Z",
            "finished_at" => "2026-10-15T15:30:10Z",
            "status" => "success",
            "output_path" => "/tmp/fundamentals_2026-10-15.parquet",
            "error_message" => nil,
            "collection_id" => "fundamentals-20261015T153000Z"
          },
          "file_metadata" => {
            "path" => "/tmp/fundamentals_2026-10-15.parquet",
            "dataset_type" => "fundamentals",
            "provider_name" => "schwab",
            "ticker" => "all",
            "frequency" => nil,
            "expiration_date" => nil,
            "row_count" => 150,
            "first_observed_at" => "2026-10-15T15:30:00Z",
            "last_observed_at" => "2026-10-15T15:30:00Z",
            "file_mtime" => 2000,
            "file_size" => 50_000,
            "updated_at" => Time.now.utc.iso8601,
            "collection_id" => "fundamentals-20261015T153000Z"
          }
        }

        sidecar_legacy = {
          "fetch_run" => {
            "job_type" => "daily_fundamentals",
            "dataset_type" => "fundamentals",
            "symbol" => nil,
            "frequency" => nil,
            "option_root" => nil,
            "requested_buckets" => nil,
            "resolved_expiration" => nil,
            "scheduled_for" => "2026-10-14T15:30:00Z",
            "started_at" => "2026-10-14T15:30:01Z",
            "finished_at" => "2026-10-14T15:30:09Z",
            "status" => "success",
            "output_path" => "/tmp/fundamentals_2026-10-14.parquet",
            "error_message" => nil,
            "collection_id" => "fundamentals-20261014T153000Z"
          },
          "file_metadata" => {
            "path" => "/tmp/fundamentals_2026-10-14.parquet",
            "dataset_type" => "fundamentals",
            "provider_name" => "schwab",
            "ticker" => "all",
            "frequency" => nil,
            "expiration_date" => nil,
            "row_count" => 148,
            "first_observed_at" => "2026-10-14T15:30:00Z",
            "last_observed_at" => "2026-10-14T15:30:00Z",
            "file_mtime" => 1900,
            "file_size" => 49_000,
            "updated_at" => Time.now.utc.iso8601,
            "collection_id" => "fundamentals-20261014T153000Z"
          }
        }

        File.write(File.join(pending_dir, "fundamentals_2026-10-15.meta.json"), JSON.generate(sidecar_new))
        File.write(File.join(pending_dir, "fundamentals_2026-10-14.meta.json"), JSON.generate(sidecar_legacy))

        app_config = instance_double(
          Tickrake::Config,
          pending_metadata_dir: pending_dir,
          database: pg_config,
          default_provider_name: "schwab",
          provider_definition: double("provider_def")
        )
        scheduled_job = Tickrake::ScheduledJobConfig.new(
          name: "metadata_sync", type: "metadata_sync", settings: { "batch_size" => 500 }
        )
        logger = instance_double(Logger, info: nil, warn: nil, error: nil, "level=": nil)
        runtime = Tickrake::Runtime.new(
          config: app_config,
          tracker: tracker,
          client_factory: double("client_factory"),
          provider_factory: double("provider_factory"),
          logger: logger
        )

        count = Tickrake::MetadataSyncJob.new(runtime, scheduled_job: scheduled_job).run
        expect(count).to eq(2)

        runs = tracker.fetch_runs
        expect(runs.length).to eq(2)
        expect(runs.map { |r| r["symbol"] }).to eq(%w[none none])

        meta15 = tracker.file_metadata("/tmp/fundamentals_2026-10-15.parquet")
        expect(meta15["ticker"]).to eq("all")
        expect(meta15["row_count"].to_i).to eq(150)

        meta14 = tracker.file_metadata("/tmp/fundamentals_2026-10-14.parquet")
        expect(meta14["ticker"]).to eq("all")
        expect(meta14["row_count"].to_i).to eq(148)

        expect(Dir.glob(File.join(pending_dir, "*.meta.json"))).to be_empty
      ensure
        tracker.close
      end
    end
  end

  describe "PostgresRateLimiter" do
    it "consumes tokens and replenishes across requests" do
      Tickrake::Tracker.migrate!(pg_config)
      adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
      limiter = Tickrake::DB::PostgresRateLimiter.new(
        adapter,
        provider: "schwab",
        capacity: 2,
        refill_rate: 1.0 # 1 token per second
      )

      expect(limiter.try_consume).to eq(true)
      expect(limiter.try_consume).to eq(true)
      expect(limiter.try_consume).to eq(false)

      # Fast forward
      row = adapter.get_first_row("SELECT tokens, last_refill_at FROM api_rate_limits WHERE provider = 'schwab'")
      adapter.execute("UPDATE api_rate_limits SET last_refill_at = $1 WHERE provider = 'schwab'", [row["last_refill_at"].to_f - 1.5])

      expect(limiter.try_consume).to eq(true)
      adapter.close
    end
  end

  describe "SqliteToPostgresMigrator" do
    it "previews source rows in dry-run mode without modifying target" do
      Dir.mktmpdir do |dir|
        sqlite_path = File.join(dir, "tickrake.sqlite3")
        tracker = Tickrake::Tracker.new(sqlite_path)
        tracker.record_start(
          job_type: "candles",
          dataset_type: "candles",
          symbol: "QQQ",
          frequency: "day",
          scheduled_for: Time.utc(2026, 10, 15, 16, 0, 0),
          started_at: Time.utc(2026, 10, 15, 16, 0, 1)
        )
        tracker.close

        pg_adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
        stdout = StringIO.new
        migrator = SqliteToPostgresMigrator.new(
          sqlite_path: sqlite_path,
          pg_adapter: pg_adapter,
          dry_run: true,
          stdout: stdout
        )

        migrator.run

        expect(stdout.string).to include("[DRY RUN]")
        expect(stdout.string).to include("fetch_runs: 1 rows")
        expect(pg_adapter.row_count("fetch_runs")).to eq(0)
        pg_adapter.close
      end
    end

    it "rejects non-empty target PostgreSQL database" do
      Dir.mktmpdir do |dir|
        sqlite_path = File.join(dir, "tickrake.sqlite3")
        tracker = Tickrake::Tracker.new(sqlite_path)
        tracker.close

        # Prepopulate target postgres
        Tickrake::Tracker.migrate!(pg_config)
        pg_tracker = Tickrake::Tracker.new(pg_config)
        pg_tracker.record_start(
          job_type: "candles",
          dataset_type: "candles",
          symbol: "AAPL",
          scheduled_for: Time.utc(2026, 10, 15, 16, 0, 0),
          started_at: Time.utc(2026, 10, 15, 16, 0, 1)
        )
        pg_tracker.close

        pg_adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
        migrator = SqliteToPostgresMigrator.new(
          sqlite_path: sqlite_path,
          pg_adapter: pg_adapter,
          dry_run: false,
          stdout: StringIO.new
        )

        expect { migrator.run }.to raise_error(Tickrake::Error, /Target PostgreSQL database is not empty/)
        pg_adapter.close
      end
    end

    it "migrates data, preserves IDs and balances, and resets sequences" do
      Dir.mktmpdir do |dir|
        sqlite_path = File.join(dir, "tickrake.sqlite3")
        sqlite_tracker = Tickrake::Tracker.new(sqlite_path)

        # 1. Fetch runs
        run_id = sqlite_tracker.record_start(
          job_type: "options",
          dataset_type: "options",
          symbol: "SPX",
          option_root: "SPXW",
          requested_buckets: [0],
          resolved_expiration: "2026-10-15",
          scheduled_for: Time.utc(2026, 10, 15, 14, 30, 0),
          started_at: Time.utc(2026, 10, 15, 14, 30, 1)
        )
        sqlite_tracker.record_finish(
          id: run_id,
          status: "success",
          finished_at: Time.utc(2026, 10, 15, 14, 30, 2),
          output_path: "/path/to/spxw.csv"
        )

        # 2. File metadata
        sqlite_tracker.upsert_file_metadata(
          path: "/path/to/spxw.csv",
          dataset_type: "options",
          provider_name: "schwab",
          ticker: "SPXW",
          row_count: 100,
          first_observed_at: "2026-10-15T14:30:00Z",
          last_observed_at: "2026-10-15T14:30:00Z",
          file_mtime: 12345,
          file_size: 4096
        )

        # 3. Market indexes & memberships
        sqlite_tracker.upsert_tickers([
          { "ticker" => "AAPL", "security_name" => "Apple Inc.", "gics_sector" => "Information Technology" }
        ])
        sqlite_tracker.replace_market_index_memberships(
          index_code: "SP500",
          index_name: "S&P 500",
          rows: [{ "ticker" => "AAPL", "start_date" => "2020-01-01", "end_date" => nil }]
        )

        # 4. Rate limits
        sqlite_limiter = Tickrake::DB::SqliteRateLimiter.new(
          sqlite_tracker,
          provider: "schwab",
          capacity: 10,
          refill_rate: 2.0
        )
        sqlite_limiter.try_consume
        sqlite_tracker.close

        pg_adapter = Tickrake::DB::Adapters::PostgresAdapter.new(pg_config)
        stdout = StringIO.new
        migrator = SqliteToPostgresMigrator.new(
          sqlite_path: sqlite_path,
          pg_adapter: pg_adapter,
          dry_run: false,
          stdout: stdout
        )

        migrator.run

        expect(stdout.string).to include("Successfully migrated SQLite state to PostgreSQL!")

        # Verify PostgreSQL rows
        pg_tracker = Tickrake::Tracker.new(pg_config)
        pg_runs = pg_tracker.fetch_runs
        expect(pg_runs.length).to eq(1)
        expect(pg_runs.first["id"].to_i).to eq(run_id)
        expect(pg_runs.first["symbol"]).to eq("SPX")

        pg_meta = pg_tracker.file_metadata("/path/to/spxw.csv")
        expect(pg_meta["ticker"]).to eq("SPXW")
        expect(pg_meta["row_count"].to_i).to eq(100)

        members = pg_tracker.members_for_index(index_code: "SP500", as_of: "2026-01-01")
        expect(members).to eq(["AAPL"])

        # Verify rate limit balance
        limit_row = pg_adapter.get_first_row("SELECT tokens FROM api_rate_limits WHERE provider = 'schwab'")
        expect(limit_row["tokens"].to_f).to be_within(0.5).of(9.0)

        # Verify sequence was reset and next insert increments properly without collision
        next_run_id = pg_tracker.record_start(
          job_type: "candles",
          dataset_type: "candles",
          symbol: "SPY",
          scheduled_for: Time.utc(2026, 10, 16, 16, 0, 0),
          started_at: Time.utc(2026, 10, 16, 16, 0, 1)
        )
        expect(next_run_id).to eq(run_id + 1)

        pg_tracker.close
        pg_adapter.close
      end
    end
  end
end
