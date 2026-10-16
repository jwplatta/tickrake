# frozen_string_literal: true

require_relative "db/connection_manager"
require_relative "models/api_rate_limit"

module Tickrake
  class RateLimiter
    class << self
      def build(config_or_path, provider:, capacity:, refill_rate:, **_ignored)
        new(config_or_path, provider: provider, capacity: capacity, refill_rate: refill_rate)
      end
    end

    def initialize(config_or_path, provider:, capacity:, refill_rate:, **_ignored)
      DB::ConnectionManager.establish_connection(config_or_path)
      @provider = provider.to_s
      @capacity = capacity.to_f
      @refill_rate = refill_rate.to_f
    end

    def try_consume
      Models::ApiRateLimit.try_consume(
        provider: @provider,
        capacity: @capacity,
        refill_rate: @refill_rate
      )
    end

    def consume!
      attempts = 0
      until try_consume
        attempts += 1
        raise Tickrake::Error, "Rate limit exhausted for provider #{@provider}" if attempts > 4

        sleep 0.5
      end
    end
  end
end
