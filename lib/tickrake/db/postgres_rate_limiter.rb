# frozen_string_literal: true

module Tickrake
  module DB
    class PostgresRateLimiter
      def initialize(adapter, provider:, capacity:, refill_rate:)
        @adapter     = adapter
        @provider    = provider
        @capacity    = capacity.to_f
        @refill_rate = refill_rate.to_f
      end

      # Non-blocking: returns true if token consumed, false if bucket empty
      def try_consume
        now = Time.now.to_f
        @adapter.transaction do
          # Ensure row exists
          @adapter.execute(
            <<~SQL,
              INSERT INTO api_rate_limits (provider, tokens, last_refill_at)
              VALUES (?, ?, ?)
              ON CONFLICT (provider) DO NOTHING
            SQL
            [@provider, @capacity, now]
          )

          row = @adapter.get_first_row(
            "SELECT tokens, last_refill_at FROM api_rate_limits WHERE provider = ? FOR UPDATE",
            [@provider]
          )

          if row
            elapsed   = [now - row["last_refill_at"].to_f, 0].max
            available = [row["tokens"].to_f + elapsed * @refill_rate, @capacity].min
          else
            available = @capacity
          end

          return false if available < 1.0

          new_tokens = [available - 1.0, 0.0].max
          @adapter.execute(
            <<~SQL,
              UPDATE api_rate_limits
              SET tokens = ?, last_refill_at = ?
              WHERE provider = ?
            SQL
            [new_tokens, now, @provider]
          )
        end
        true
      rescue StandardError => e
        # If lock couldn't be acquired or serialization failure
        false
      end

      # Blocking: retries up to ~2s before raising
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
end
