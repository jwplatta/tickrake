# frozen_string_literal: true

module Tickrake
  module DB
    module RateLimiter
      class << self
        def build(config, provider:, capacity:, refill_rate:, adapter: nil, logger: nil)
          db_config = config.respond_to?(:database) ? config.database : nil
          if db_config&.postgres? || adapter.is_a?(Tickrake::DB::Adapters::PostgresAdapter)
            pg_adapter = adapter || Tickrake::DB::Adapters.build(config)
            PostgresRateLimiter.new(
              pg_adapter,
              provider: provider,
              capacity: capacity,
              refill_rate: refill_rate,
              logger: logger
            )
          else
            SqliteRateLimiter.new(
              adapter&.respond_to?(:path) ? adapter.path : config,
              provider: provider,
              capacity: capacity,
              refill_rate: refill_rate,
              logger: logger
            )
          end
        end
      end
    end
  end
end
