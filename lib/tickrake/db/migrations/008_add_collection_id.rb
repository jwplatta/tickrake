# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddCollectionId
        def self.version
          8
        end

        def initialize(database)
          @database = database
        end

        def up
          add_column_unless_exists("fetch_runs", "collection_id", "TEXT") if table_exists?("fetch_runs")
          add_column_unless_exists("file_metadata_cache", "collection_id", "TEXT") if table_exists?("file_metadata_cache")

          if table_exists?("fetch_runs")
            create_index_unless_exists(
              "idx_fetch_runs_collection",
              "fetch_runs",
              "(dataset_type, option_root, collection_id)"
            )
          end

          if table_exists?("file_metadata_cache")
            create_index_unless_exists(
              "idx_file_metadata_collection",
              "file_metadata_cache",
              "(dataset_type, provider_name, ticker, collection_id)"
            )
          end
        end

        private

        def table_exists?(table)
          @database.get_first_value(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            [table]
          )
        end

        def add_column_unless_exists(table, column, sql_type)
          columns = @database.table_info(table).map { |row| row["name"] }
          return if columns.include?(column)

          @database.execute("ALTER TABLE #{table} ADD COLUMN #{column} #{sql_type}")
        end

        def create_index_unless_exists(index_name, table, columns)
          exists = @database.get_first_value(
            "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
            [index_name]
          )
          return if exists

          @database.execute("CREATE INDEX #{index_name} ON #{table} #{columns}")
        end
      end
    end
  end
end
