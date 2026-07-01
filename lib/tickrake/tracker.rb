# frozen_string_literal: true

require "monitor"

module Tickrake
  class Tracker
    SQLITE_BUSY_TIMEOUT_MS = 10_000

    FILE_METADATA_COLUMNS = %w[
      path
      dataset_type
      provider_name
      ticker
      frequency
      expiration_date
      storage_format
      storage_location
      artifact_status
      remote_uri
      source_file_count
      row_count
      first_observed_at
      last_observed_at
      file_mtime
      file_size
      updated_at
    ].freeze

    def self.migrate!(path)
      tracker = new(path, migrate: true)
      tracker.close
      nil
    end

    def self.migrations
      [
        Tickrake::DB::Migrations::CreateFetchRuns,
        Tickrake::DB::Migrations::CreateFileMetadataCache,
        Tickrake::DB::Migrations::AddFetchRunsFrequency,
        Tickrake::DB::Migrations::AddOptionExpirationAndIndexes,
        Tickrake::DB::Migrations::AddOptionTickerTimeIndex,
        Tickrake::DB::Migrations::CreateMarketIndexTables,
        Tickrake::DB::Migrations::AddArtifactMetadataToFileCache
      ].freeze
    end

    def initialize(path, migrate: false)
      @path = Tickrake::PathSupport.expand_path(path)
      @db_lock = Monitor.new
      FileUtils.mkdir_p(File.dirname(@path))
      migrate ? migrate! : ensure_schema_current!
    end

    def close
      synchronize_db do
        return unless defined?(@db) && @db

        @db.close
        @db = nil
      end
    end

    def record_start(attrs)
      synchronize_db do
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
    end

    def record_finish(id:, status:, finished_at:, output_path: nil, error_message: nil)
      synchronize_db do
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
    end

    def fetch_runs
      synchronize_db { db.execute("SELECT * FROM fetch_runs ORDER BY id") }
    end

    def file_metadata(path)
      synchronize_db do
        db.get_first_row("SELECT * FROM file_metadata_cache WHERE path = ?", [Tickrake::PathSupport.expand_path(path)])
      end
    end

    def file_metadata_aggregate(where: nil, binds: [])
      base = "SELECT COUNT(*) AS file_count, COALESCE(SUM(file_size), 0) AS total_bytes, MIN(last_observed_at) AS oldest_observed_at, MAX(last_observed_at) AS newest_observed_at FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      synchronize_db { db.get_first_row(sql, binds) }
    end

    def file_metadata_aggregate_by_provider(where: nil, binds: [])
      base = "SELECT provider_name, COUNT(*) AS file_count, COALESCE(SUM(file_size), 0) AS total_bytes, MIN(last_observed_at) AS oldest_observed_at, MAX(last_observed_at) AS newest_observed_at FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      sql += " GROUP BY provider_name ORDER BY provider_name"
      synchronize_db { db.execute(sql, binds) }
    end

    def file_metadata_largest(where: nil, binds: [], limit: 5)
      base = "SELECT path, file_size FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      sql += " ORDER BY file_size DESC LIMIT #{Integer(limit)}"
      synchronize_db { db.execute(sql, binds) }
    end

    def file_metadata_rows(where: nil, binds: [], order_by: nil, limit: nil)
      sql = +"SELECT * FROM file_metadata_cache"
      sql << " WHERE #{where}" if where && !where.empty?
      sql << " ORDER BY #{order_by}" if order_by && !order_by.empty?
      sql << " LIMIT #{Integer(limit)}" if limit
      synchronize_db { db.execute(sql, binds) }
    end

    def upsert_file_metadata(attrs)
      bulk_upsert_file_metadata([attrs])
    end

    def bulk_upsert_file_metadata(attrs_list)
      return if attrs_list.empty?

      with_transaction do
        attrs_list.each do |attrs|
          values = normalized_file_metadata_values(attrs)
          db.execute(
            <<~SQL,
              INSERT INTO file_metadata_cache (
                #{FILE_METADATA_COLUMNS.join(", ")}
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(path) DO UPDATE SET
                dataset_type = excluded.dataset_type,
                provider_name = excluded.provider_name,
                ticker = excluded.ticker,
                frequency = excluded.frequency,
                expiration_date = excluded.expiration_date,
                storage_format = excluded.storage_format,
                storage_location = excluded.storage_location,
                artifact_status = excluded.artifact_status,
                remote_uri = excluded.remote_uri,
                source_file_count = excluded.source_file_count,
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
      end
    end

    def delete_file_metadata_paths(paths)
      normalized_paths = Array(paths).map { |path| Tickrake::PathSupport.expand_path(path) }.uniq
      return 0 if normalized_paths.empty?

      placeholders = (["?"] * normalized_paths.length).join(", ")
      synchronize_db do
        with_transaction do
          db.execute("DELETE FROM file_metadata_cache WHERE path IN (#{placeholders})", normalized_paths)
        end
        db.changes
      end
    end

    def upsert_tickers(rows)
      return if rows.empty?

      timestamp = iso(Time.now)
      synchronize_db do
        rows.each do |row|
          db.execute(
            <<~SQL,
              INSERT INTO tickers (
                ticker, security_name, gics_sector, gics_sub_industry,
                headquarters_location, cik, founded, status, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(ticker) DO UPDATE SET
                security_name = excluded.security_name,
                gics_sector = excluded.gics_sector,
                gics_sub_industry = excluded.gics_sub_industry,
                headquarters_location = excluded.headquarters_location,
                cik = excluded.cik,
                founded = excluded.founded,
                status = excluded.status,
                updated_at = excluded.updated_at
            SQL
            [
              row.fetch("ticker"),
              row["security_name"],
              row["gics_sector"],
              row["gics_sub_industry"],
              row["headquarters_location"],
              row["cik"],
              row["founded"],
              row["status"],
              timestamp,
              timestamp
            ]
          )
        end
      end
    end

    def replace_ticker_aliases(rows)
      return if rows.empty?

      synchronize_db do
        tickers = rows.map { |row| row.fetch("ticker") }.uniq
        ensure_tickers_for_aliases(tickers)
        ticker_ids = ticker_id_map(tickers)
        placeholders = (["?"] * ticker_ids.length).join(", ")
        db.execute("DELETE FROM ticker_aliases WHERE ticker_id IN (#{placeholders})", ticker_ids.values)

        timestamp = iso(Time.now)
        rows.each do |row|
          db.execute(
            <<~SQL,
              INSERT INTO ticker_aliases (
                ticker_id, alias_ticker, start_date, end_date, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?)
            SQL
            [
              ticker_ids.fetch(row.fetch("ticker")),
              row.fetch("alias_ticker"),
              row["start_date"],
              row["end_date"],
              timestamp,
              timestamp
            ]
          )
        end
      end
    end

    def replace_market_index_memberships(index_code:, index_name:, rows:)
      synchronize_db do
        market_index_id = upsert_market_index(index_code: index_code, index_name: index_name)
        db.execute("DELETE FROM market_index_memberships WHERE market_index_id = ?", [market_index_id])

        ensure_tickers_for_memberships(rows)
        ticker_ids = ticker_id_map(rows.map { |row| row.fetch("ticker") })
        timestamp = iso(Time.now)
        rows.each do |row|
          db.execute(
            <<~SQL,
              INSERT INTO market_index_memberships (
                market_index_id, ticker_id, start_date, end_date, created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?)
            SQL
            [
              market_index_id,
              ticker_ids.fetch(row.fetch("ticker")),
              row.fetch("start_date"),
              row["end_date"],
              timestamp,
              timestamp
            ]
          )
        end
      end
    end

    def members_for_index(index_code:, as_of:)
      synchronize_db do
        db.execute(
          <<~SQL,
            SELECT tickers.ticker
            FROM market_index_memberships memberships
            INNER JOIN market_indexes indexes
              ON indexes.id = memberships.market_index_id
            INNER JOIN tickers
              ON tickers.id = memberships.ticker_id
            WHERE indexes.code = ?
              AND memberships.start_date <= ?
              AND (memberships.end_date IS NULL OR memberships.end_date >= ?)
            ORDER BY tickers.ticker ASC
          SQL
          [index_code, as_of, as_of]
        ).map { |row| row.fetch("ticker") }
      end
    end

    def with_transaction
      synchronize_db do
        db.transaction
        yield
        db.commit
      rescue StandardError
        db.rollback if db.transaction_active?
        raise
      end
    end

    private

    def synchronize_db(&block)
      @db_lock.synchronize(&block)
    end

    def db
      @db ||= SQLite3::Database.new(@path).tap do |database|
        database.busy_timeout(SQLITE_BUSY_TIMEOUT_MS)
        database.execute("PRAGMA journal_mode = WAL")
        database.results_as_hash = true
      end
    end

    def normalized_file_metadata_values(attrs)
      path = Tickrake::PathSupport.expand_path(attrs.fetch(:path))
      {
        "path" => path,
        "dataset_type" => attrs.fetch(:dataset_type),
        "provider_name" => attrs.fetch(:provider_name),
        "ticker" => attrs.fetch(:ticker),
        "frequency" => attrs[:frequency],
        "expiration_date" => attrs[:expiration_date],
        "storage_format" => attrs[:storage_format],
        "storage_location" => attrs[:storage_location],
        "artifact_status" => attrs[:artifact_status],
        "remote_uri" => attrs[:remote_uri],
        "source_file_count" => attrs[:source_file_count],
        "row_count" => Integer(attrs.fetch(:row_count)),
        "first_observed_at" => attrs[:first_observed_at],
        "last_observed_at" => attrs[:last_observed_at],
        "file_mtime" => attrs.fetch(:file_mtime).to_i,
        "file_size" => attrs.fetch(:file_size).to_i,
        "updated_at" => iso(attrs[:updated_at] || Time.now)
      }
    end

    def migrate!
      Tickrake::DB::Migrator.new(db, migrations: self.class.migrations).migrate!
    end

    def iso(value)
      value&.utc&.iso8601
    end

    def ensure_schema_current!
      raise Tickrake::Error, pending_migrations_message unless File.exist?(@path)

      applied_versions = db.execute("SELECT version FROM schema_migrations ORDER BY version").map do |row|
        row.fetch("version").to_i
      end
      pending_versions = self.class.migrations.map(&:version) - applied_versions
      raise Tickrake::Error, pending_migrations_message unless pending_versions.empty?
    rescue SQLite3::SQLException
      raise Tickrake::Error, pending_migrations_message
    end

    def pending_migrations_message
      "Database migrations are pending for #{@path}. Run `tickrake migrate`."
    end

    def upsert_market_index(index_code:, index_name:)
      timestamp = iso(Time.now)
      db.execute(
        <<~SQL,
          INSERT INTO market_indexes (code, name, created_at, updated_at)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(code) DO UPDATE SET
            name = excluded.name,
            updated_at = excluded.updated_at
        SQL
        [index_code, index_name, timestamp, timestamp]
      )
      db.get_first_value("SELECT id FROM market_indexes WHERE code = ?", [index_code])
    end

    def ensure_tickers_for_memberships(rows)
      ensure_tickers_for_aliases(rows.map { |row| row.fetch("ticker") })
    end

    def ensure_tickers_for_aliases(tickers)
      tickers = tickers.uniq
      return if tickers.empty?

      timestamp = iso(Time.now)
      tickers.each do |ticker|
        db.execute(
          <<~SQL,
            INSERT INTO tickers (ticker, created_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(ticker) DO NOTHING
          SQL
          [ticker, timestamp, timestamp]
        )
      end
    end

    def ticker_id_map(tickers)
      return {} if tickers.empty?

      placeholders = (["?"] * tickers.uniq.length).join(", ")
      rows = db.execute(
        "SELECT id, ticker FROM tickers WHERE ticker IN (#{placeholders})",
        tickers.uniq
      )
      rows.each_with_object({}) do |row, memo|
        memo[row.fetch("ticker")] = row.fetch("id")
      end
    end
  end
end
