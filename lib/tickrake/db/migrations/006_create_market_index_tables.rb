# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateMarketIndexTables
        def self.version
          6
        end

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(
            <<~SQL
              CREATE TABLE IF NOT EXISTS market_indexes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
              );

              CREATE TABLE IF NOT EXISTS tickers (
                canonical_ticker TEXT PRIMARY KEY,
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

              CREATE TABLE IF NOT EXISTS ticker_alias_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_ticker TEXT NOT NULL,
                alias_ticker TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                alias_status TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_ticker, alias_ticker, start_date)
              );

              CREATE TABLE IF NOT EXISTS market_index_memberships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_index_id INTEGER NOT NULL,
                canonical_ticker TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(market_index_id, canonical_ticker, start_date),
                FOREIGN KEY (market_index_id) REFERENCES market_indexes(id)
              );

              CREATE INDEX IF NOT EXISTS idx_market_indexes_code
              ON market_indexes (code);

              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_index_dates_ticker
              ON market_index_memberships (market_index_id, start_date, end_date, canonical_ticker);

              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_ticker_dates
              ON market_index_memberships (canonical_ticker, start_date, end_date);

              CREATE INDEX IF NOT EXISTS idx_ticker_alias_history_alias_dates
              ON ticker_alias_history (alias_ticker, start_date, end_date);

              CREATE INDEX IF NOT EXISTS idx_ticker_alias_history_canonical_dates
              ON ticker_alias_history (canonical_ticker, start_date, end_date);
            SQL
          )
        end
      end
    end
  end
end
