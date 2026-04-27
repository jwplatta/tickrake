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

  it "backfills expiration_date for existing option metadata rows during migration" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "tickrake.sqlite3")
      db = SQLite3::Database.new(path)
      db.execute_batch(
        <<~SQL
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
end
