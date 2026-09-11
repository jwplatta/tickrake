# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class DropStreamingEventTables
        def self.version = 13

        def initialize(database)
          @database = database
        end

        def up
          @database.execute("DROP TABLE IF EXISTS order_book_events")
          @database.execute("DROP TABLE IF EXISTS level_one_events")
          @database.execute("DROP TABLE IF EXISTS job_sessions")
        end
      end
    end
  end
end
