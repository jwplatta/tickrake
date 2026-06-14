# frozen_string_literal: true

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

    def file_metadata_aggregate(where: nil, binds: [])
      base = "SELECT COUNT(*) AS file_count, COALESCE(SUM(file_size), 0) AS total_bytes, MIN(last_observed_at) AS oldest_observed_at, MAX(last_observed_at) AS newest_observed_at FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      db.get_first_row(sql, binds)
    end

    def file_metadata_aggregate_by_provider(where: nil, binds: [])
      base = "SELECT provider_name, COUNT(*) AS file_count, COALESCE(SUM(file_size), 0) AS total_bytes, MIN(last_observed_at) AS oldest_observed_at, MAX(last_observed_at) AS newest_observed_at FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      sql += " GROUP BY provider_name ORDER BY provider_name"
      db.execute(sql, binds)
    end

    def file_metadata_largest(where: nil, binds: [], limit: 5)
      base = "SELECT path, file_size FROM file_metadata_cache"
      sql = where && !where.empty? ? "#{base} WHERE #{where}" : base
      sql += " ORDER BY file_size DESC LIMIT #{Integer(limit)}"
      db.execute(sql, binds)
    end

    def file_metadata_rows(where: nil, binds: [], order_by: nil, limit: nil)
      sql = +"SELECT * FROM file_metadata_cache"
      sql << " WHERE #{where}" if where && !where.empty?
      sql << " ORDER BY #{order_by}" if order_by && !order_by.empty?
      sql << " LIMIT #{Integer(limit)}" if limit
      db.execute(sql, binds)
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
      end
    end

    def upsert_tickers(rows)
      return if rows.empty?

      timestamp = iso(Time.now)
      rows.each do |row|
        db.execute(
          <<~SQL,
            INSERT INTO tickers (
              canonical_ticker, security_name, gics_sector, gics_sub_industry,
              headquarters_location, cik, founded, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_ticker) DO UPDATE SET
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
            row.fetch("canonical_ticker"),
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

    def replace_ticker_alias_history(rows)
      return if rows.empty?

      canonical_tickers = rows.map { |row| row.fetch("canonical_ticker") }.uniq
      placeholders = (["?"] * canonical_tickers.length).join(", ")
      db.execute("DELETE FROM ticker_alias_history WHERE canonical_ticker IN (#{placeholders})", canonical_tickers)

      timestamp = iso(Time.now)
      rows.each do |row|
        db.execute(
          <<~SQL,
            INSERT INTO ticker_alias_history (
              canonical_ticker, alias_ticker, start_date, end_date, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
          SQL
          [
            row.fetch("canonical_ticker"),
            row.fetch("alias_ticker"),
            row["start_date"],
            row["end_date"],
            timestamp,
            timestamp
          ]
        )
      end
    end

    def replace_market_index_memberships(index_code:, index_name:, rows:)
      market_index_id = upsert_market_index(index_code: index_code, index_name: index_name)
      db.execute("DELETE FROM market_index_memberships WHERE market_index_id = ?", [market_index_id])

      ensure_tickers_for_memberships(rows)
      ticker_ids = ticker_id_map(rows.map { |row| row.fetch("canonical_ticker") })
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
            ticker_ids.fetch(row.fetch("canonical_ticker")),
            row.fetch("start_date"),
            row["end_date"],
            timestamp,
            timestamp
          ]
        )
      end
    end

    def members_for_index(index_code:, as_of:)
      db.execute(
        <<~SQL,
          SELECT tickers.canonical_ticker
          FROM market_index_memberships memberships
          INNER JOIN market_indexes indexes
            ON indexes.id = memberships.market_index_id
          INNER JOIN tickers
            ON tickers.id = memberships.ticker_id
          WHERE indexes.code = ?
            AND memberships.start_date <= ?
            AND (memberships.end_date IS NULL OR memberships.end_date >= ?)
          ORDER BY tickers.canonical_ticker ASC
        SQL
        [index_code, as_of, as_of]
      ).map { |row| row.fetch("canonical_ticker") }
    end

    def with_transaction
      db.transaction
      yield
      db.commit
    rescue StandardError
      db.rollback if db.transaction_active?
      raise
    end

    private

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
        "row_count" => Integer(attrs.fetch(:row_count)),
        "first_observed_at" => attrs[:first_observed_at],
        "last_observed_at" => attrs[:last_observed_at],
        "file_mtime" => attrs.fetch(:file_mtime).to_i,
        "file_size" => attrs.fetch(:file_size).to_i,
        "updated_at" => iso(attrs[:updated_at] || Time.now)
      }
    end

    def migrate!
      Tickrake::DB::Migrator.new(
        db,
        migrations: [
          Tickrake::DB::Migrations::CreateFetchRuns,
          Tickrake::DB::Migrations::CreateFileMetadataCache,
          Tickrake::DB::Migrations::AddFetchRunsFrequency,
          Tickrake::DB::Migrations::AddOptionExpirationAndIndexes,
          Tickrake::DB::Migrations::AddOptionTickerTimeIndex,
          Tickrake::DB::Migrations::CreateMarketIndexTables,
          Tickrake::DB::Migrations::AddTickerIdsToIndexMemberships
        ]
      ).migrate!
    end

    def iso(value)
      value&.utc&.iso8601
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
      canonical_tickers = rows.map { |row| row.fetch("canonical_ticker") }.uniq
      return if canonical_tickers.empty?

      timestamp = iso(Time.now)
      canonical_tickers.each do |canonical_ticker|
        db.execute(
          <<~SQL,
            INSERT INTO tickers (canonical_ticker, created_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(canonical_ticker) DO NOTHING
          SQL
          [canonical_ticker, timestamp, timestamp]
        )
      end
    end

    def ticker_id_map(canonical_tickers)
      return {} if canonical_tickers.empty?

      placeholders = (["?"] * canonical_tickers.uniq.length).join(", ")
      rows = db.execute(
        "SELECT id, canonical_ticker FROM tickers WHERE canonical_ticker IN (#{placeholders})",
        canonical_tickers.uniq
      )
      rows.each_with_object({}) do |row, memo|
        memo[row.fetch("canonical_ticker")] = row.fetch("id")
      end
    end
  end
end
