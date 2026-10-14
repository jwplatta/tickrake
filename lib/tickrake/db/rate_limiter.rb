# frozen_string_literal: true

module Tickrake
  module DB
    module RateLimiter
      class << self
        def build(config, provider:, capacity:, refill_rate:, adapter: nil)
          db_config = config.database
          if db_config&.postgres?
            pg_adapter = adapter || Tickrake::DB::Adapters.build(config)
            PostgresRateLimiter.new(
              pg_adapter,
              provider: provider,
              capacity: capacity,
              refill_rate: refill_rate
            )
          else
            SqliteRateLimiter.new(
              config,
              provider: provider,
              capacity: capacity,
              refill_rate: refill_rate
            )
          end
        end
      end
    end
  end
end
