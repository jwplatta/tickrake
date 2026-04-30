# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddOptionTickerTimeIndex
        def self.version
          5
        end

        def initialize(database)
          @database = database
        end

        def up
          add_column_unless_exists("file_metadata_cache", "expiration_date", "TEXT")
          @database.execute(
            <<~SQL
              CREATE INDEX IF NOT EXISTS idx_file_metadata_options_ticker_time_lookup
              ON file_metadata_cache (dataset_type, provider_name, ticker, last_observed_at, expiration_date)
            SQL
          )
        end

        private

        def add_column_unless_exists(table, column, sql_type)
          columns = @database.table_info(table).map { |row| row["name"] }
          return if columns.include?(column)

          @database.execute("ALTER TABLE #{table} ADD COLUMN #{column} #{sql_type}")
        end
      end
    end
  end
end
