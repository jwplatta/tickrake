# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddArtifactMetadataToFileCache
        COLUMNS = {
          "storage_format" => "TEXT",
          "storage_location" => "TEXT",
          "artifact_status" => "TEXT",
          "remote_uri" => "TEXT",
          "source_file_count" => "INTEGER"
        }.freeze

        def self.version
          7
        end

        def initialize(database)
          @database = database
        end

        def up
          COLUMNS.each do |column, sql_type|
            add_column_unless_exists("file_metadata_cache", column, sql_type)
          end
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
