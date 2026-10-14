# frozen_string_literal: true

module Tickrake
  module DB
    module PostgresMigrations
      class CreateSchema
        def self.version
          1
        end

        def initialize(adapter)
          @adapter = adapter
        end

        def up
          @adapter.execute_batch(
            <<~SQL
              -- fetch_runs
              CREATE TABLE IF NOT EXISTS fetch_runs (
                id BIGSERIAL PRIMARY KEY,
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
                error_message TEXT,
                collection_id TEXT
              );

              CREATE INDEX IF NOT EXISTS idx_fetch_runs_collection
              ON fetch_runs (dataset_type, option_root, collection_id);

              -- file_metadata_cache
              CREATE TABLE IF NOT EXISTS file_metadata_cache (
                path TEXT PRIMARY KEY,
                dataset_type TEXT NOT NULL,
                provider_name TEXT NOT NULL,
                ticker TEXT NOT NULL,
                frequency TEXT,
                expiration_date TEXT,
                row_count BIGINT NOT NULL,
                first_observed_at TEXT,
                last_observed_at TEXT,
                file_mtime BIGINT NOT NULL,
                file_size BIGINT NOT NULL,
                updated_at TEXT NOT NULL,
                collection_id TEXT
              );

              CREATE INDEX IF NOT EXISTS idx_file_metadata_candles_lookup
              ON file_metadata_cache (dataset_type, provider_name, ticker, frequency);

              CREATE INDEX IF NOT EXISTS idx_file_metadata_options_lookup
              ON file_metadata_cache (dataset_type, provider_name, expiration_date, last_observed_at);

              CREATE INDEX IF NOT EXISTS idx_file_metadata_options_ticker_time_lookup
              ON file_metadata_cache (dataset_type, provider_name, ticker, last_observed_at, expiration_date);

              CREATE INDEX IF NOT EXISTS idx_file_metadata_collection
              ON file_metadata_cache (dataset_type, provider_name, ticker, collection_id);

              -- market_indexes
              CREATE TABLE IF NOT EXISTS market_indexes (
                id BIGSERIAL PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
              );

              CREATE INDEX IF NOT EXISTS idx_market_indexes_code
              ON market_indexes (code);

              -- tickers
              CREATE TABLE IF NOT EXISTS tickers (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT NOT NULL UNIQUE,
                security_name TEXT,
                gics_sector TEXT,
                gics_sub_industry TEXT,
                headquarters_location TEXT,
                cik TEXT,
                founded TEXT,
                status TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
              );

              -- ticker_aliases
              CREATE TABLE IF NOT EXISTS ticker_aliases (
                id BIGSERIAL PRIMARY KEY,
                ticker_id BIGINT NOT NULL REFERENCES tickers(id),
                alias_ticker TEXT NOT NULL UNIQUE,
                start_date TEXT,
                end_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
              );

              CREATE INDEX IF NOT EXISTS idx_ticker_aliases_alias_dates
              ON ticker_aliases (alias_ticker, start_date, end_date);

              CREATE INDEX IF NOT EXISTS idx_ticker_aliases_ticker_dates
              ON ticker_aliases (ticker_id, start_date, end_date);

              -- market_index_memberships
              CREATE TABLE IF NOT EXISTS market_index_memberships (
                id BIGSERIAL PRIMARY KEY,
                market_index_id BIGINT NOT NULL REFERENCES market_indexes(id),
                ticker_id BIGINT NOT NULL REFERENCES tickers(id),
                start_date TEXT NOT NULL,
                end_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(market_index_id, ticker_id, start_date)
              );

              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_index_dates_ticker
              ON market_index_memberships (market_index_id, start_date, end_date, ticker_id);

              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_ticker_dates
              ON market_index_memberships (ticker_id, start_date, end_date);

              -- api_rate_limits
              CREATE TABLE IF NOT EXISTS api_rate_limits (
                provider TEXT PRIMARY KEY,
                tokens DOUBLE PRECISION NOT NULL,
                last_refill_at DOUBLE PRECISION NOT NULL
              );
            SQL
          )
        end
      end
    end
  end
end
