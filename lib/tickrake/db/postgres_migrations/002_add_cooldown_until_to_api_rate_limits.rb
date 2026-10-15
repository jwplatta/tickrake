# frozen_string_literal: true

module Tickrake
  module DB
    module PostgresMigrations
      class AddCooldownUntilToApiRateLimits
        def self.version
          2
        end

        def initialize(adapter)
          @adapter = adapter
        end

        def up
          @adapter.execute(
            "ALTER TABLE api_rate_limits ADD COLUMN IF NOT EXISTS cooldown_until DOUBLE PRECISION"
          )
        end
      end
    end
  end
end
