# frozen_string_literal: true

require "time"

module Tickrake
  module DB
    class SqliteRateLimiter
      DEFAULT_MAX_WAIT_SECONDS = 120.0
      DEFAULT_RETRY_INTERVAL = 0.25
      MAX_COOLDOWN_SECONDS = 300.0

      attr_reader :provider, :capacity, :refill_rate, :logger

      def initialize(config_or_path, provider:, capacity:, refill_rate:, logger: nil)
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
        @logger      = logger

        ensure_schema!
      end

      # Non-blocking: returns true if token consumed, false if bucket empty or in cooldown
      def try_consume
        now = Time.now.to_f
        @db.transaction(:immediate) do
          row = @db.get_first_row(
            "SELECT tokens, last_refill_at, cooldown_until FROM api_rate_limits WHERE provider = ?",
            [@provider]
          )
          if row
            cooldown_until = row["cooldown_until"] ? row["cooldown_until"].to_f : 0.0
            return false if cooldown_until > now

            elapsed   = [now - row["last_refill_at"].to_f, 0.0].max
            available = [row["tokens"].to_f + (elapsed * @refill_rate), @capacity].min
          else
            cooldown_until = nil
            available = @capacity
          end

          return false if available < 1.0

          new_tokens = [available - 1.0, 0.0].max
          @db.execute(
            <<~SQL,
              INSERT INTO api_rate_limits (provider, tokens, last_refill_at, cooldown_until)
              VALUES (?, ?, ?, ?)
              ON CONFLICT(provider) DO UPDATE SET
                tokens = excluded.tokens,
                last_refill_at = excluded.last_refill_at
            SQL
            [@provider, new_tokens, now, cooldown_until]
          )
        end
        true
      rescue SQLite3::BusyException
        false
      end

      # Blocking: retries acquisition up to timeout (default 120s) before raising RateLimitWaitTimeoutError
      def consume!(timeout: DEFAULT_MAX_WAIT_SECONDS, retry_interval: DEFAULT_RETRY_INTERVAL)
        start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        deadline = start_time + timeout
        attempts = 0

        loop do
          attempts += 1
          if try_consume
            if attempts > 1
              elapsed_ms = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time) * 1000).round
              emit_visibility(
                msg: "rate_limit_waited",
                event: "rate_limit_waited",
                provider: @provider,
                wait_duration_ms: elapsed_ms,
                attempts: attempts
              )
            end
            emit_visibility(
              msg: "provider_request",
              event: "provider_request",
              provider: @provider
            )
            return true
          end

          now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          if now >= deadline
            elapsed = now - start_time
            emit_visibility(
              msg: "rate_limit_timeout",
              event: "rate_limit_timeout",
              provider: @provider,
              elapsed_s: elapsed.round(2),
              timeout_s: timeout
            )
            raise Tickrake::RateLimitWaitTimeoutError.new(
              provider: @provider,
              elapsed_seconds: elapsed,
              timeout_seconds: timeout
            )
          end

          sleep_time = [retry_interval, deadline - now].min
          sleep(sleep_time) if sleep_time > 0
        end
      end

      # Applies a provider-wide cooldown based on Retry-After or fallback
      def apply_cooldown!(retry_after = nil, fallback: 30.0)
        cooldown_seconds = parse_retry_after(retry_after, fallback: fallback)
        now = Time.now.to_f
        cooldown_until = now + cooldown_seconds

        @db.transaction(:immediate) do
          @db.execute(
            <<~SQL,
              INSERT INTO api_rate_limits (provider, tokens, last_refill_at, cooldown_until)
              VALUES (?, ?, ?, ?)
              ON CONFLICT(provider) DO UPDATE SET
                cooldown_until = MAX(COALESCE(api_rate_limits.cooldown_until, 0), excluded.cooldown_until)
            SQL
            [@provider, @capacity, now, cooldown_until]
          )
        end

        emit_visibility(
          msg: "provider_cooldown",
          event: "provider_cooldown",
          provider: @provider,
          cooldown_seconds: cooldown_seconds.round(2),
          retry_after: retry_after&.to_s
        )
        cooldown_seconds
      rescue SQLite3::BusyException
        sleep 0.05
        retry
      end

      # Returns remaining cooldown duration in seconds
      def current_cooldown
        row = @db.get_first_row(
          "SELECT cooldown_until FROM api_rate_limits WHERE provider = ?",
          [@provider]
        )
        return 0.0 unless row && row["cooldown_until"]

        [row["cooldown_until"].to_f - Time.now.to_f, 0.0].max
      end

      private

      def ensure_schema!
        @db.execute(<<~SQL)
          CREATE TABLE IF NOT EXISTS api_rate_limits (
            provider       TEXT PRIMARY KEY,
            tokens         REAL NOT NULL,
            last_refill_at REAL NOT NULL,
            cooldown_until REAL
          );
        SQL
      rescue SQLite3::BusyException
        # Table exists or is busy being initialized
      end

      def parse_retry_after(retry_after, fallback: 30.0)
        fallback = [fallback.to_f, 1.0].max
        return fallback if retry_after.nil?

        str = retry_after.to_s.strip
        return fallback if str.empty?

        seconds = if str =~ /\A\d+(\.\d+)?\z/
                    str.to_f
                  else
                    begin
                      Time.httpdate(str) - Time.now
                    rescue ArgumentError
                      fallback
                    end
                  end

        if seconds <= 0
          fallback
        else
          [seconds, MAX_COOLDOWN_SECONDS].min
        end
      end

      def emit_visibility(payload)
        return unless @logger

        level = case payload[:event]
                when "rate_limit_timeout", "provider_request_failure"
                  :error
                when "provider_cooldown", "rate_limit_429"
                  :warn
                else
                  :info
                end
        @logger.public_send(level, payload)
      end
    end
  end
end
