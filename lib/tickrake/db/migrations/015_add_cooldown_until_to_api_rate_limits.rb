# frozen_string_literal: true

module Tickrake
  module DB
    module Migrations
      class AddCooldownUntilToApiRateLimits
        def self.version = 15

        def initialize(database)
          @database = database
        end

        def up
          columns = @database.execute("PRAGMA table_info(api_rate_limits)").map { |col| col["name"] }
          unless columns.include?("cooldown_until")
            @database.execute("ALTER TABLE api_rate_limits ADD COLUMN cooldown_until REAL")
          end
        end
      end
    end
  end
end
