# frozen_string_literal: true

RSpec.describe Tickrake::Tracker do
  it "records run lifecycle rows in sqlite" do
    Dir.mktmpdir do |dir|
      tracker = described_class.new(File.join(dir, "tickrake.sqlite3"))
      id = tracker.record_start(
        job_type: "options_monitor",
        dataset_type: "options",
        symbol: "SPY",
        option_root: nil,
        requested_buckets: [0, 1],
        resolved_expiration: "2025-07-21",
        scheduled_for: Time.utc(2025, 7, 20, 14, 30, 0),
        started_at: Time.utc(2025, 7, 20, 14, 30, 1)
      )

      tracker.record_finish(id: id, status: "success", finished_at: Time.utc(2025, 7, 20, 14, 30, 2), output_path: "/tmp/out.csv")
      row = tracker.fetch_runs.first

      expect(row["status"]).to eq("success")
      expect(row["output_path"]).to eq("/tmp/out.csv")
      expect(row["requested_buckets"]).to include("0")
    end
  end

  it "stores file metadata cache rows keyed by path" do
    Dir.mktmpdir do |dir|
      tracker = described_class.new(File.join(dir, "tickrake.sqlite3"))
      path = File.join(dir, "history", "ibkr-paper", "SPY_1min.csv")

      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "candles",
        provider_name: "ibkr-paper",
        ticker: "SPY",
        frequency: "1min",
        row_count: 120,
        first_observed_at: "2026-04-11T13:30:00Z",
        last_observed_at: "2026-04-11T15:29:00Z",
        file_mtime: 1_744_462_800,
        file_size: 4096
      )

      row = tracker.file_metadata(path)

      expect(row["dataset_type"]).to eq("candles")
      expect(row["provider_name"]).to eq("ibkr-paper")
      expect(row["ticker"]).to eq("SPY")
      expect(row["frequency"]).to eq("1min")
      expect(row["row_count"]).to eq(120)
    end
  end

  it "configures sqlite for WAL and busy timeout" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      tracker = described_class.new(path)
      database = tracker.send(:db)

      expect(database.get_first_value("PRAGMA journal_mode").to_s.downcase).to eq("wal")
      expect(database.get_first_value("PRAGMA busy_timeout")).to eq(described_class::SQLITE_BUSY_TIMEOUT_MS)
    end
  end

  it "bulk upserts multiple metadata rows in one transaction" do
    Dir.mktmpdir do |dir|
      tracker = described_class.new(File.join(dir, "tickrake.sqlite3"))
      tracker.bulk_upsert_file_metadata(
        [
          {
            path: File.join(dir, "history", "ibkr-paper", "SPY_1min.csv"),
            dataset_type: "candles",
            provider_name: "ibkr-paper",
            ticker: "SPY",
            frequency: "1min",
            row_count: 120,
            first_observed_at: "2026-04-11T13:30:00Z",
            last_observed_at: "2026-04-11T15:29:00Z",
            file_mtime: 1_744_462_800,
            file_size: 4096
          },
          {
            path: File.join(dir, "options", "massive", "SPXW_exp2026-04-11_2026-04-11_13-30-00.csv"),
            dataset_type: "options",
            provider_name: "massive",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-11",
            row_count: 42,
            first_observed_at: "2026-04-11T13:30:00Z",
            last_observed_at: "2026-04-11T13:30:00Z",
            file_mtime: 1_744_462_801,
            file_size: 2048
          }
        ]
      )

      rows = tracker.file_metadata_rows(order_by: "path")

      expect(rows.length).to eq(2)
      expect(rows.map { |row| row["path"] }).to all(include(dir))
    end
  end

  it "bulk upsert updates existing metadata rows on conflict" do
    Dir.mktmpdir do |dir|
      tracker = described_class.new(File.join(dir, "tickrake.sqlite3"))
      path = File.join(dir, "options", "massive", "SPXW_exp2026-04-11_2026-04-11_13-30-00.csv")

      tracker.upsert_file_metadata(
        path: path,
        dataset_type: "options",
        provider_name: "massive",
        ticker: "SPXW",
        frequency: nil,
        expiration_date: "2026-04-11",
        row_count: 1,
        first_observed_at: "2026-04-11T13:30:00Z",
        last_observed_at: "2026-04-11T13:30:00Z",
        file_mtime: 1,
        file_size: 100
      )

      tracker.bulk_upsert_file_metadata(
        [
          {
            path: path,
            dataset_type: "options",
            provider_name: "massive",
            ticker: "SPXW",
            frequency: nil,
            expiration_date: "2026-04-11",
            row_count: 99,
            first_observed_at: "2026-04-11T13:30:00Z",
            last_observed_at: "2026-04-11T13:31:00Z",
            file_mtime: 2,
            file_size: 200
          }
        ]
      )

      row = tracker.file_metadata(path)

      expect(row["row_count"]).to eq(99)
      expect(row["last_observed_at"]).to eq("2026-04-11T13:31:00Z")
      expect(row["file_size"]).to eq(200)
    end
  end

  it "allows batched metadata writes while another connection holds a read transaction" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      tracker = described_class.new(path)
      reader = SQLite3::Database.new(path)
      reader.results_as_hash = true
      reader.execute("PRAGMA journal_mode = WAL")
      reader.transaction
      reader.execute("SELECT * FROM file_metadata_cache")

      expect do
        tracker.bulk_upsert_file_metadata(
          [
            {
              path: File.join(dir, "options", "massive", "SPXW_exp2026-04-11_2026-04-11_13-30-00.csv"),
              dataset_type: "options",
              provider_name: "massive",
              ticker: "SPXW",
              frequency: nil,
              expiration_date: "2026-04-11",
              row_count: 42,
              first_observed_at: "2026-04-11T13:30:00Z",
              last_observed_at: "2026-04-11T13:30:00Z",
              file_mtime: 1_744_462_801,
              file_size: 2048
            }
          ]
        )
      end.not_to raise_error
    ensure
      reader&.rollback if reader&.transaction_active?
      reader&.close
    end
  end

  it "records completed migration versions when opening the database" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      described_class.new(path)
      db = SQLite3::Database.new(path)
      versions = db.execute("SELECT version FROM schema_migrations ORDER BY version").flatten

      expect(versions).to eq([1, 2, 3, 4])
    ensure
      db&.close
    end
  end

  it "backfills expiration_date for existing option metadata rows during migration" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      db = SQLite3::Database.new(path)
      db.execute_batch(
        <<~SQL
          CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY
          );

          CREATE TABLE file_metadata_cache (
            path TEXT PRIMARY KEY,
            dataset_type TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            ticker TEXT NOT NULL,
            frequency TEXT,
            row_count INTEGER NOT NULL,
            first_observed_at TEXT,
            last_observed_at TEXT,
            file_mtime INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            updated_at TEXT NOT NULL
          );
        SQL
      )
      db.execute("INSERT INTO schema_migrations (version) VALUES (2)")
      db.execute(
        <<~SQL,
          INSERT INTO file_metadata_cache (
            path, dataset_type, provider_name, ticker, frequency, row_count,
            first_observed_at, last_observed_at, file_mtime, file_size, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        SQL
        [
          File.join(dir, "options", "schwab", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"),
          "options",
          "schwab",
          "SPXW",
          nil,
          1,
          "2026-04-10T14:30:00Z",
          "2026-04-10T14:30:00Z",
          1,
          128,
          "2026-04-10T14:30:00Z"
        ]
      )
      db.execute(
        <<~SQL,
          INSERT INTO file_metadata_cache (
            path, dataset_type, provider_name, ticker, frequency, row_count,
            first_observed_at, last_observed_at, file_mtime, file_size, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        SQL
        [
          File.join(dir, "history", "ibkr-paper", "SPY_1min.csv"),
          "candles",
          "ibkr-paper",
          "SPY",
          "1min",
          120,
          "2026-04-10T13:30:00Z",
          "2026-04-10T15:29:00Z",
          1,
          4096,
          "2026-04-10T15:29:00Z"
        ]
      )
      db.close

      tracker = described_class.new(path)
      option_row = tracker.file_metadata(File.join(dir, "options", "schwab", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"))
      candle_row = tracker.file_metadata(File.join(dir, "history", "ibkr-paper", "SPY_1min.csv"))

      expect(option_row["expiration_date"]).to eq("2026-04-17")
      expect(candle_row["expiration_date"]).to eq(nil)

      tracker_again = described_class.new(path)
      option_row_again = tracker_again.file_metadata(File.join(dir, "options", "schwab", "SPXW_exp2026-04-17_2026-04-10_14-30-00.csv"))

      expect(option_row_again["expiration_date"]).to eq("2026-04-17")
    end
  end

  it "creates query indexes for file metadata lookups during migration" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      db = SQLite3::Database.new(path)
      db.execute_batch(
        <<~SQL
          CREATE TABLE schema_migrations (
            version INTEGER PRIMARY KEY
          );

          CREATE TABLE file_metadata_cache (
            path TEXT PRIMARY KEY,
            dataset_type TEXT NOT NULL,
            provider_name TEXT NOT NULL,
            ticker TEXT NOT NULL,
            frequency TEXT,
            expiration_date TEXT,
            row_count INTEGER NOT NULL,
            first_observed_at TEXT,
            last_observed_at TEXT,
            file_mtime INTEGER NOT NULL,
            file_size INTEGER NOT NULL,
            updated_at TEXT NOT NULL
          );
        SQL
      )
      db.execute("INSERT INTO schema_migrations (version) VALUES (1)")
      db.execute("INSERT INTO schema_migrations (version) VALUES (2)")
      db.execute("INSERT INTO schema_migrations (version) VALUES (3)")
      db.close

      tracker = described_class.new(path)
      db = SQLite3::Database.new(path)
      index_names = db.execute("SELECT name FROM sqlite_master WHERE type = 'index'").flatten

      expect(index_names).to include("idx_file_metadata_candles_lookup")
      expect(index_names).to include("idx_file_metadata_options_lookup")

      described_class.new(path)
      index_names_again = db.execute("SELECT name FROM sqlite_master WHERE type = 'index'").flatten

      expect(index_names_again).to include("idx_file_metadata_candles_lookup")
      expect(index_names_again).to include("idx_file_metadata_options_lookup")
    ensure
      db&.close
    end
  end
end
