# frozen_string_literal: true

module Tickrake
  class Tracker
    def initialize(path)
      @path = Tickrake::PathSupport.expand_path(path)
      FileUtils.mkdir_p(File.dirname(@path))
      migrate!
    end

    def record_start(attrs)
      db.execute(
        <<~SQL,
          INSERT INTO fetch_runs (
            job_type, dataset_type, symbol, option_root, requested_buckets,
            resolved_expiration, scheduled_for, started_at, status, output_path, error_message
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        SQL
        [
          attrs.fetch(:job_type),
          attrs.fetch(:dataset_type),
          attrs.fetch(:symbol),
          attrs[:option_root],
          attrs[:requested_buckets] && JSON.dump(attrs[:requested_buckets]),
          attrs[:resolved_expiration],
          iso(attrs[:scheduled_for]),
          iso(attrs.fetch(:started_at)),
          "running",
          attrs[:output_path],
          nil
        ]
      )
      db.last_insert_row_id
    end

    def record_finish(id:, status:, finished_at:, output_path: nil, error_message: nil)
      db.execute(
        <<~SQL,
          UPDATE fetch_runs
          SET status = ?, finished_at = ?, output_path = COALESCE(?, output_path), error_message = ?
          WHERE id = ?
        SQL
        [
          status,
          iso(finished_at),
          output_path,
          error_message,
          id
        ]
      )
    end

    def fetch_runs
      db.execute("SELECT * FROM fetch_runs ORDER BY id")
    end

    private

    def db
      @db ||= SQLite3::Database.new(@path).tap do |database|
        database.results_as_hash = true
      end
    end

    def migrate!
      db.execute_batch(
        <<~SQL
          CREATE TABLE IF NOT EXISTS fetch_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_type TEXT NOT NULL,
            dataset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            option_root TEXT,
            requested_buckets TEXT,
            resolved_expiration TEXT,
            scheduled_for TEXT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            output_path TEXT,
            error_message TEXT
          );
        SQL
      )
    end

    def iso(value)
      value&.utc&.iso8601
    end
  end
end
