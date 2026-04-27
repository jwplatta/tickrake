# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateFileMetadataCache
        def self.version
          2
        end

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(
            <<~SQL
              CREATE TABLE IF NOT EXISTS file_metadata_cache (
                path TEXT PRIMARY KEY,
                dataset_type TEXT NOT NULL,
                provider_name TEXT NOT NULL,
                ticker TEXT NOT NULL,
                frequency TEXT,
                row_count INTEGER NOT NULL,
                first_observed_at TEXT,
                last_observed_at TEXT,
                file_mtime INTEGER NOT NULL,
                file_size INTEGER NOT NULL,
                updated_at TEXT NOT NULL
              );
            SQL
          )
        end
      end
    end
  end
end
