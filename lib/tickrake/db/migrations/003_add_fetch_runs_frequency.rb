# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddFetchRunsFrequency
        def self.version
          3
        end

        def initialize(database)
          @database = database
        end

        def up
          columns = @database.table_info("fetch_runs").map { |row| row["name"] }
          return if columns.include?("frequency")

          @database.execute("ALTER TABLE fetch_runs ADD COLUMN frequency TEXT")
        end
      end
    end
  end
end
