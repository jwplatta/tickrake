# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddOptionExpirationAndIndexes
        def self.version
          4
        end

        def initialize(database)
          @database = database
        end

        def up
          add_column_unless_exists("file_metadata_cache", "expiration_date", "TEXT")
          create_index_unless_exists(
            "idx_file_metadata_candles_lookup",
            "file_metadata_cache",
            "dataset_type, provider_name, ticker, frequency"
          )
          create_index_unless_exists(
            "idx_file_metadata_options_lookup",
            "file_metadata_cache",
            "dataset_type, provider_name, expiration_date, last_observed_at"
          )
          backfill_option_expiration_dates
        end

        private

        def add_column_unless_exists(table, column, sql_type)
          columns = @database.table_info(table).map { |row| row["name"] }
          return if columns.include?(column)

          @database.execute("ALTER TABLE #{table} ADD COLUMN #{column} #{sql_type}")
        end

        def create_index_unless_exists(name, table, columns)
          @database.execute("CREATE INDEX IF NOT EXISTS #{name} ON #{table} (#{columns})")
        end

        def backfill_option_expiration_dates
          rows = @database.execute(
            "SELECT path FROM file_metadata_cache WHERE dataset_type = ? AND expiration_date IS NULL",
            ["options"]
          )
          rows.each do |row|
            expiration_date = expiration_date_from_path(row.fetch("path"))
            next unless expiration_date

            @database.execute(
              "UPDATE file_metadata_cache SET expiration_date = ? WHERE path = ? AND expiration_date IS NULL",
              [expiration_date, row.fetch("path")]
            )
          end
        end

        def expiration_date_from_path(path)
          basename = File.basename(path.to_s, ".csv")
          match = /_exp(?<expiration_date>\d{4}-\d{2}-\d{2})_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\z/.match(basename)
          match && match[:expiration_date]
        end
      end
    end
  end
end
