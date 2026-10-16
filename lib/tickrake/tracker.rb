# frozen_string_literal: true

require "monitor"
require "json"
require "time"

module Tickrake
  class Tracker
    SQLITE_BUSY_TIMEOUT_MS = 30_000

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
      collection_id
    ].freeze

    class << self
      def migrate!(config_or_path = nil)
        Tickrake::DB::Migrator.migrate!(config_or_path)
        nil
      end

      def migrations(_backend = "sqlite")
        [Tickrake::DB::Migrator]
      end
    end

    attr_reader :config_or_path, :path

    def initialize(config_or_path = nil, migrate: false)
      @config_or_path = config_or_path
      @db_config = Tickrake::DB::ConnectionManager.resolve_config(config_or_path)
      @path = @db_config.sqlite? ? Tickrake::PathSupport.expand_path(@db_config.sqlite_path || Tickrake::PathSupport.sqlite_path) : nil
      @lock = Monitor.new

      Tickrake::DB::ConnectionManager.establish_connection(config_or_path)

      if migrate
        migrate!
      else
        ensure_schema_current!
      end
    end

    def close
      # Managed by ActiveRecord connection pool
    end

    def checkpoint!
      if sqlite?
        ActiveRecord::Base.connection.execute("PRAGMA wal_checkpoint(PASSIVE)") rescue nil
      end
    end

    def record_start(attrs)
      synchronize do
        symbol = (attrs[:symbol].nil? || attrs[:symbol].to_s.strip.empty?) ? "none" : attrs[:symbol]
        requested_buckets = attrs[:requested_buckets] ? JSON.dump(attrs[:requested_buckets]) : nil

        run = Models::FetchRun.create!(
          job_type: attrs.fetch(:job_type),
          dataset_type: attrs.fetch(:dataset_type),
          symbol: symbol,
          frequency: attrs[:frequency],
          option_root: attrs[:option_root],
          requested_buckets: requested_buckets,
          resolved_expiration: attrs[:resolved_expiration],
          scheduled_for: iso(attrs[:scheduled_for]),
          started_at: iso(attrs.fetch(:started_at)),
          status: "running",
          output_path: attrs[:output_path],
          collection_id: attrs[:collection_id]
        )
        run.id
      end
    end

    def record_finish(id:, status:, finished_at:, output_path: nil, error_message: nil)
      synchronize do
        run = Models::FetchRun.find(id)
        updates = {
          status: status,
          finished_at: iso(finished_at),
          error_message: error_message
        }
        updates[:output_path] = output_path if output_path
        run.update!(updates)
      end
    end

    def ingest_sidecars(fetch_runs:, metadata_list:)
      with_transaction do
        bulk_insert_fetch_runs(fetch_runs) if fetch_runs.any?
        bulk_upsert_file_metadata(metadata_list) if metadata_list.any?
      end
    end

    def bulk_insert_fetch_runs(attrs_list)
      return if attrs_list.empty?

      synchronize do
        records = attrs_list.map do |attrs|
          symbol = (attrs["symbol"].nil? || attrs["symbol"].to_s.strip.empty?) ? "none" : attrs["symbol"]
          requested_buckets = attrs["requested_buckets"]
          requested_buckets = JSON.dump(requested_buckets) if requested_buckets.is_a?(Array) || requested_buckets.is_a?(Hash)

          {
            job_type: attrs.fetch("job_type"),
            dataset_type: attrs.fetch("dataset_type"),
            symbol: symbol,
            frequency: attrs["frequency"],
            option_root: attrs["option_root"],
            requested_buckets: requested_buckets,
            resolved_expiration: attrs["resolved_expiration"],
            scheduled_for: attrs["scheduled_for"],
            started_at: attrs.fetch("started_at"),
            finished_at: attrs["finished_at"],
            status: attrs.fetch("status"),
            output_path: attrs["output_path"],
            error_message: attrs["error_message"],
            collection_id: attrs["collection_id"]
          }
        end
        Models::FetchRun.insert_all(records)
      end
    end

    def fetch_runs
      synchronize do
        Models::FetchRun.order(:id).as_json
      end
    end

    def file_metadata(path)
      synchronize do
        expanded = Tickrake::PathSupport.expand_path(path)
        record = Models::FileMetadata.where(path: expanded).take
        record&.as_json
      end
    end

    def file_metadata_aggregate(where: nil, binds: [])
      synchronize do
        Models::FileMetadata.aggregate_summary(where: where, binds: binds)
      end
    end

    def file_metadata_aggregate_by_provider(where: nil, binds: [])
      synchronize do
        Models::FileMetadata.aggregate_by_provider(where: where, binds: binds)
      end
    end

    def file_metadata_largest(where: nil, binds: [], limit: 5)
      synchronize do
        Models::FileMetadata.largest(where: where, binds: binds, limit: limit)
      end
    end

    def file_metadata_rows(where: nil, binds: [], order_by: nil, limit: nil)
      synchronize do
        Models::FileMetadata.query_rows(where: where, binds: binds, order_by: order_by, limit: limit)
      end
    end

    def upsert_file_metadata(attrs)
      bulk_upsert_file_metadata([attrs])
    end

    def evict_stale_metadata(older_than_days:)
      synchronize do
        cutoff = (Time.now.utc - (older_than_days * 86_400)).iso8601
        Models::FileMetadata.where("updated_at < ?", cutoff).delete_all
      end
    end

    def bulk_upsert_file_metadata(attrs_list)
      return if attrs_list.empty?

      synchronize do
        records = attrs_list.map { |attrs| normalized_file_metadata_values(attrs) }
        Models::FileMetadata.upsert_all(records, unique_by: :path)
      end
    end

    def intraday_index_rows(provider_name:, root:, date: nil)
      synchronize do
        Models::FileMetadata.intraday_index_rows(provider_name: provider_name, root: root, date: date)
      end
    end

    def intraday_series_rows(provider_name:, root:, date: nil)
      synchronize do
        Models::FileMetadata.intraday_series_rows(provider_name: provider_name, root: root, date: date)
      end
    end

    def collection_file_count(collection_id:, provider_name:, root:)
      synchronize do
        Models::FileMetadata.where(
          dataset_type: "options",
          provider_name: provider_name,
          ticker: root,
          collection_id: collection_id
        ).count
      end
    end

    def intraday_active_roots(date: nil)
      synchronize do
        Models::FileMetadata.intraday_active_roots(date: date)
      end
    end

    def delete_file_metadata_paths(paths)
      normalized_paths = Array(paths).map { |p| Tickrake::PathSupport.expand_path(p) }.uniq
      return 0 if normalized_paths.empty?

      synchronize do
        Models::FileMetadata.where(path: normalized_paths).delete_all
      end
    end

    def upsert_tickers(rows)
      return if rows.empty?

      timestamp = iso(Time.now)
      synchronize do
        records = rows.map do |row|
          {
            "ticker" => row.fetch("ticker"),
            "security_name" => row["security_name"],
            "gics_sector" => row["gics_sector"],
            "gics_sub_industry" => row["gics_sub_industry"],
            "headquarters_location" => row["headquarters_location"],
            "cik" => row["cik"],
            "founded" => row["founded"],
            "status" => row["status"],
            "created_at" => timestamp,
            "updated_at" => timestamp
          }
        end
        Models::Ticker.upsert_all(records, unique_by: :ticker)
      end
    end

    def replace_ticker_aliases(rows)
      return if rows.empty?

      synchronize do
        tickers = rows.map { |r| r.fetch("ticker") }.uniq
        ensure_tickers_for_aliases(tickers)
        ticker_ids = ticker_id_map(tickers)

        timestamp = iso(Time.now)
        with_transaction do
          Models::TickerAlias.where(ticker_id: ticker_ids.values).delete_all
          records = rows.map do |row|
            {
              "ticker_id" => ticker_ids.fetch(row.fetch("ticker")),
              "alias_ticker" => row.fetch("alias_ticker"),
              "start_date" => row["start_date"],
              "end_date" => row["end_date"],
              "created_at" => timestamp,
              "updated_at" => timestamp
            }
          end
          Models::TickerAlias.insert_all(records)
        end
      end
    end

    def replace_market_index_memberships(index_code:, index_name:, rows:)
      synchronize do
        timestamp = iso(Time.now)
        with_transaction do
          market_index = Models::MarketIndex.where(code: index_code).take ||
                         Models::MarketIndex.create!(code: index_code, name: index_name, created_at: timestamp, updated_at: timestamp)
          market_index.update!(name: index_name, updated_at: timestamp)

          Models::MarketIndexMembership.where(market_index_id: market_index.id).delete_all

          ensure_tickers_for_memberships(rows)
          ticker_ids = ticker_id_map(rows.map { |r| r.fetch("ticker") })

          if rows.any?
            records = rows.map do |row|
              {
                "market_index_id" => market_index.id,
                "ticker_id" => ticker_ids.fetch(row.fetch("ticker")),
                "start_date" => row.fetch("start_date"),
                "end_date" => row["end_date"],
                "created_at" => timestamp,
                "updated_at" => timestamp
              }
            end
            Models::MarketIndexMembership.insert_all(records)
          end
        end
      end
    end

    def members_for_index(index_code:, as_of:)
      synchronize do
        Models::Ticker.joins(memberships: :market_index)
                      .where(market_indexes: { code: index_code })
                      .where("market_index_memberships.start_date <= ?", as_of)
                      .where("market_index_memberships.end_date IS NULL OR market_index_memberships.end_date >= ?", as_of)
                      .order(:ticker)
                      .pluck(:ticker)
      end
    end

    def with_transaction(&block)
      ActiveRecord::Base.transaction(&block)
    end

    def db
      conn = ActiveRecord::Base.connection
      unless conn.singleton_class.method_defined?(:get_first_value)
        def conn.get_first_value(sql, _binds = [])
          select_value(sql)
        end
        def conn.get_first_row(sql, _binds = [])
          select_one(sql)
        end
        def conn.table_info(table_name)
          execute("PRAGMA table_info(#{table_name})")
        end
      end
      conn
    end

    private

    def synchronize(&block)
      @lock.synchronize(&block)
    end

    def postgres?
      @db_config.postgres?
    end

    def sqlite?
      @db_config.sqlite?
    end

    def migrate!
      Tickrake::DB::Migrator.migrate!(@config_or_path)
    end

    def ensure_schema_current!
      if sqlite? && @path && !File.exist?(@path)
        raise Tickrake::Error, pending_migrations_message
      end

      if Tickrake::DB::Migrator.pending?(@config_or_path)
        raise Tickrake::Error, pending_migrations_message
      end
    rescue StandardError => e
      raise e if e.is_a?(Tickrake::Error)

      raise Tickrake::Error, pending_migrations_message
    end

    def pending_migrations_message
      target_desc = @path || "database"
      "Database migrations are pending for #{target_desc}. Run `tickrake migrate`."
    end

    def iso(value)
      case value
      when Time, DateTime
        value.utc.iso8601
      when Date
        value.iso8601
      else
        value
      end
    end

    def normalized_file_metadata_values(attrs)
      path = Tickrake::PathSupport.expand_path(attrs[:path] || attrs["path"])
      {
        "path" => path,
        "dataset_type" => attrs[:dataset_type] || attrs["dataset_type"],
        "provider_name" => attrs[:provider_name] || attrs["provider_name"],
        "ticker" => attrs[:ticker] || attrs["ticker"],
        "frequency" => attrs[:frequency] || attrs["frequency"],
        "expiration_date" => attrs[:expiration_date] || attrs["expiration_date"],
        "row_count" => Integer(attrs[:row_count] || attrs["row_count"]),
        "first_observed_at" => attrs[:first_observed_at] || attrs["first_observed_at"],
        "last_observed_at" => attrs[:last_observed_at] || attrs["last_observed_at"],
        "file_mtime" => (attrs[:file_mtime] || attrs["file_mtime"]).to_i,
        "file_size" => (attrs[:file_size] || attrs["file_size"]).to_i,
        "updated_at" => iso(attrs[:updated_at] || attrs["updated_at"] || Time.now),
        "collection_id" => attrs[:collection_id] || attrs["collection_id"]
      }
    end

    def ensure_tickers_for_memberships(rows)
      ensure_tickers_for_aliases(rows.map { |r| r.fetch("ticker") })
    end

    def ensure_tickers_for_aliases(tickers)
      tickers = tickers.uniq
      return if tickers.empty?

      timestamp = iso(Time.now)
      records = tickers.map do |ticker|
        {
          "ticker" => ticker,
          "created_at" => timestamp,
          "updated_at" => timestamp
        }
      end
      Models::Ticker.upsert_all(records, unique_by: :ticker)
    end

    def ticker_id_map(tickers)
      return {} if tickers.empty?

      Models::Ticker.where(ticker: tickers.uniq).pluck(:ticker, :id).to_h
    end
  end
end
