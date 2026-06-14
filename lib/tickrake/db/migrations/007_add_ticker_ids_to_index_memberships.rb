# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddTickerIdsToIndexMemberships
        def self.version
          7
        end

        def initialize(database)
          @database = database
        end

        def up
          return if upgraded_schema?

          @database.execute("PRAGMA foreign_keys = OFF")

          create_tickers_v2
          copy_existing_tickers
          backfill_membership_only_tickers
          recreate_ticker_alias_history

          @database.execute("ALTER TABLE market_index_memberships RENAME TO market_index_memberships_old")
          @database.execute("DROP TABLE tickers")
          @database.execute("ALTER TABLE tickers_v2 RENAME TO tickers")

          create_market_index_memberships_v2
          copy_existing_memberships
          @database.execute("DROP TABLE market_index_memberships_old")

          create_indexes
        ensure
          @database.execute("PRAGMA foreign_keys = ON")
        end

        private

        def upgraded_schema?
          ticker_columns = @database.table_info("tickers").map { |row| row["name"] }
          membership_columns = @database.table_info("market_index_memberships").map { |row| row["name"] }
          alias_columns = @database.table_info("ticker_alias_history").map { |row| row["name"] }
          ticker_columns.include?("id") &&
            membership_columns.include?("ticker_id") &&
            !membership_columns.include?("canonical_ticker") &&
            !alias_columns.include?("alias_status") &&
            !alias_columns.include?("notes")
        end

        def create_tickers_v2
          @database.execute_batch(
            <<~SQL
              CREATE TABLE tickers_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_ticker TEXT NOT NULL UNIQUE,
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
            SQL
          )
        end

        def copy_existing_tickers
          @database.execute_batch(
            <<~SQL
              INSERT INTO tickers_v2 (
                canonical_ticker, security_name, gics_sector, gics_sub_industry,
                headquarters_location, cik, founded, status, created_at, updated_at
              )
              SELECT
                canonical_ticker, security_name, gics_sector, gics_sub_industry,
                headquarters_location, cik, founded, status, created_at, updated_at
              FROM tickers
              ORDER BY canonical_ticker;
            SQL
          )
        end

        def backfill_membership_only_tickers
          timestamp = Time.now.utc.iso8601
          @database.execute(
            <<~SQL,
              INSERT INTO tickers_v2 (canonical_ticker, created_at, updated_at)
              SELECT DISTINCT memberships.canonical_ticker, ?, ?
              FROM market_index_memberships memberships
              LEFT JOIN tickers_v2 tickers
                ON tickers.canonical_ticker = memberships.canonical_ticker
              WHERE tickers.id IS NULL
              ORDER BY memberships.canonical_ticker
            SQL
            [timestamp, timestamp]
          )
        end

        def create_market_index_memberships_v2
          @database.execute_batch(
            <<~SQL
              CREATE TABLE market_index_memberships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_index_id INTEGER NOT NULL,
                ticker_id INTEGER NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(market_index_id, ticker_id, start_date),
                FOREIGN KEY (market_index_id) REFERENCES market_indexes(id),
                FOREIGN KEY (ticker_id) REFERENCES tickers(id)
              );
            SQL
          )
        end

        def recreate_ticker_alias_history
          @database.execute("ALTER TABLE ticker_alias_history RENAME TO ticker_alias_history_old")
          @database.execute_batch(
            <<~SQL
              CREATE TABLE ticker_alias_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_ticker TEXT NOT NULL,
                alias_ticker TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(canonical_ticker, alias_ticker, start_date)
              );
            SQL
          )
          @database.execute_batch(
            <<~SQL
              INSERT INTO ticker_alias_history (
                canonical_ticker, alias_ticker, start_date, end_date, created_at, updated_at
              )
              SELECT
                canonical_ticker, alias_ticker, start_date, end_date, created_at, updated_at
              FROM ticker_alias_history_old;
            SQL
          )
          @database.execute("DROP TABLE ticker_alias_history_old")
        end

        def copy_existing_memberships
          @database.execute_batch(
            <<~SQL
              INSERT INTO market_index_memberships (
                market_index_id, ticker_id, start_date, end_date, created_at, updated_at
              )
              SELECT
                memberships.market_index_id,
                tickers.id,
                memberships.start_date,
                memberships.end_date,
                memberships.created_at,
                memberships.updated_at
              FROM market_index_memberships_old memberships
              INNER JOIN tickers
                ON tickers.canonical_ticker = memberships.canonical_ticker;
            SQL
          )
        end

        def create_indexes
          @database.execute("CREATE INDEX IF NOT EXISTS idx_market_indexes_code ON market_indexes (code)")
          @database.execute(
            <<~SQL
              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_index_dates_ticker
              ON market_index_memberships (market_index_id, start_date, end_date, ticker_id)
            SQL
          )
          @database.execute(
            <<~SQL
              CREATE INDEX IF NOT EXISTS idx_market_index_memberships_ticker_dates
              ON market_index_memberships (ticker_id, start_date, end_date)
            SQL
          )
          @database.execute(
            <<~SQL
              CREATE INDEX IF NOT EXISTS idx_ticker_alias_history_alias_dates
              ON ticker_alias_history (alias_ticker, start_date, end_date)
            SQL
          )
          @database.execute(
            <<~SQL
              CREATE INDEX IF NOT EXISTS idx_ticker_alias_history_canonical_dates
              ON ticker_alias_history (canonical_ticker, start_date, end_date)
            SQL
          )
        end
      end
    end
  end
end
