# frozen_string_literal: true

module Tickrake
  class Tracker
    FILE_METADATA_COLUMNS = %w[
      path
      dataset_type
      provider_name
      ticker
      frequency
      expiration_date
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

    def file_metadata_rows(where: nil, binds: [], order_by: nil)
      sql = +"SELECT * FROM file_metadata_cache"
      sql << " WHERE #{where}" if where && !where.empty?
      sql << " ORDER BY #{order_by}" if order_by && !order_by.empty?
      db.execute(sql, binds)
    end

    def upsert_file_metadata(attrs)
      path = Tickrake::PathSupport.expand_path(attrs.fetch(:path))
      values = {
        "path" => path,
        "dataset_type" => attrs.fetch(:dataset_type),
        "provider_name" => attrs.fetch(:provider_name),
        "ticker" => attrs.fetch(:ticker),
        "frequency" => attrs[:frequency],
        "expiration_date" => attrs[:expiration_date],
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
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(path) DO UPDATE SET
            dataset_type = excluded.dataset_type,
            provider_name = excluded.provider_name,
            ticker = excluded.ticker,
            frequency = excluded.frequency,
            expiration_date = excluded.expiration_date,
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
      Tickrake::DB::Migrator.new(
        db,
        migrations: [
          Tickrake::DB::Migrations::CreateFetchRuns,
          Tickrake::DB::Migrations::CreateFileMetadataCache,
          Tickrake::DB::Migrations::AddFetchRunsFrequency,
          Tickrake::DB::Migrations::AddOptionExpirationAndIndexes
        ]
      ).migrate!
    end

    def iso(value)
      value&.utc&.iso8601
    end
  end
end
