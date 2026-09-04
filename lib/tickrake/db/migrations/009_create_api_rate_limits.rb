# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class CreateApiRateLimits
        def self.version = 9

        def initialize(database)
          @database = database
        end

        def up
          @database.execute_batch(<<~SQL)
            CREATE TABLE IF NOT EXISTS api_rate_limits (
              provider       TEXT PRIMARY KEY,
              tokens         REAL NOT NULL,
              last_refill_at REAL NOT NULL
            );
          SQL
        end
      end
    end
  end
end
