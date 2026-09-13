# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class RemoveArchiveMetadataFromFileCache
        def self.version = 14

        def initialize(database)
          @database = database
        end

        def up
          @database.execute("ALTER TABLE file_metadata_cache DROP COLUMN storage_format")
          @database.execute("ALTER TABLE file_metadata_cache DROP COLUMN storage_location")
          @database.execute("ALTER TABLE file_metadata_cache DROP COLUMN artifact_status")
          @database.execute("ALTER TABLE file_metadata_cache DROP COLUMN remote_uri")
          @database.execute("ALTER TABLE file_metadata_cache DROP COLUMN source_file_count")
        end
      end
    end
  end
end
