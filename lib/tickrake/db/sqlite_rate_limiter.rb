# frozen_string_literal: true

module Tickrake
  module DB
    class SqliteRateLimiter
      def initialize(config_or_path, provider:, capacity:, refill_rate:)
        path = if config_or_path.is_a?(String)
                 config_or_path
               elsif config_or_path.respond_to?(:sqlite_path)
                 config_or_path.sqlite_path
               elsif config_or_path.respond_to?(:path)
                 config_or_path.path
               else
                 Tickrake::PathSupport.sqlite_path
               end
        @db          = Tickrake::DB.connection(path)
        @provider    = provider
        @capacity    = capacity.to_f
        @refill_rate = refill_rate.to_f
      end

      # Non-blocking: returns true if token consumed, false if bucket empty
      def try_consume
        now = Time.now.to_f
        @db.transaction(:immediate) do
          row = @db.get_first_row(
            "SELECT tokens, last_refill_at FROM api_rate_limits WHERE provider = ?",
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
          @db.execute(
            "INSERT OR REPLACE INTO api_rate_limits (provider, tokens, last_refill_at) VALUES (?, ?, ?)",
            [@provider, new_tokens, now]
          )
        end
        true
      rescue SQLite3::BusyException
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
