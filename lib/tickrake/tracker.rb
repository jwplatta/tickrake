# frozen_string_literal: true

module Tickrake
  class Tracker
    FILE_METADATA_COLUMNS = %w[
      path
      dataset_type
      provider_name
      ticker
      frequency
      row_count
      first_observed_at
      last_observed_at
      file_mtime
      file_size
      updated_at
    ].freeze

    def initialize(path)
      @path = Tickrake::PathSupport.expand_path(path)
      FileUtils.mkdir_p(File.dirname(@path))
      migrate!
    end

    def record_start(attrs)
      db.execute(
        <<~SQL,
          INSERT INTO fetch_runs (
            job_type, dataset_type, symbol, frequency, option_root, requested_buckets,
            resolved_expiration, scheduled_for, started_at, status, output_path, error_message
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        SQL
        [
          attrs.fetch(:job_type),
          attrs.fetch(:dataset_type),
          attrs.fetch(:symbol),
          attrs[:frequency],
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

    def file_metadata(path)
      db.get_first_row("SELECT * FROM file_metadata_cache WHERE path = ?", [Tickrake::PathSupport.expand_path(path)])
    end

    def upsert_file_metadata(attrs)
      path = Tickrake::PathSupport.expand_path(attrs.fetch(:path))
      values = {
        "path" => path,
        "dataset_type" => attrs.fetch(:dataset_type),
        "provider_name" => attrs.fetch(:provider_name),
        "ticker" => attrs.fetch(:ticker),
        "frequency" => attrs[:frequency],
        "row_count" => Integer(attrs.fetch(:row_count)),
        "first_observed_at" => attrs[:first_observed_at],
        "last_observed_at" => attrs[:last_observed_at],
        "file_mtime" => attrs.fetch(:file_mtime).to_i,
        "file_size" => attrs.fetch(:file_size).to_i,
        "updated_at" => iso(attrs[:updated_at] || Time.now)
      }

      db.execute(
        <<~SQL,
          INSERT INTO file_metadata_cache (
            #{FILE_METADATA_COLUMNS.join(", ")}
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(path) DO UPDATE SET
            dataset_type = excluded.dataset_type,
            provider_name = excluded.provider_name,
            ticker = excluded.ticker,
            frequency = excluded.frequency,
            row_count = excluded.row_count,
            first_observed_at = excluded.first_observed_at,
            last_observed_at = excluded.last_observed_at,
            file_mtime = excluded.file_mtime,
            file_size = excluded.file_size,
            updated_at = excluded.updated_at
        SQL
        FILE_METADATA_COLUMNS.map { |column| values.fetch(column) }
      )
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
            frequency TEXT,
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

          CREATE TABLE IF NOT EXISTS file_metadata_cache (
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
      add_column_unless_exists("fetch_runs", "frequency", "TEXT")
    end

    def add_column_unless_exists(table, column, sql_type)
      columns = db.table_info(table).map { |row| row["name"] }
      return if columns.include?(column)

      db.execute("ALTER TABLE #{table} ADD COLUMN #{column} #{sql_type}")
    end

    def iso(value)
      value&.utc&.iso8601
    end
  end
end
