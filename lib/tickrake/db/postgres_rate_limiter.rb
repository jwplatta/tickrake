# frozen_string_literal: true

require "time"

module Tickrake
  module DB
    class PostgresRateLimiter
      DEFAULT_MAX_WAIT_SECONDS = 120.0
      DEFAULT_RETRY_INTERVAL = 0.25
      MAX_COOLDOWN_SECONDS = 300.0

      attr_reader :provider, :capacity, :refill_rate, :logger

      def initialize(adapter, provider:, capacity:, refill_rate:, logger: nil)
        @adapter     = adapter
        @provider    = provider
        @capacity    = capacity.to_f
        @refill_rate = refill_rate.to_f
        @logger      = logger

        ensure_schema!
      end

      # Non-blocking: returns true if token consumed, false if bucket empty or in cooldown
      def try_consume
        now = Time.now.to_f
        @adapter.transaction do
          # Ensure row exists
          @adapter.execute(
            <<~SQL,
              INSERT INTO api_rate_limits (provider, tokens, last_refill_at, cooldown_until)
              VALUES (?, ?, ?, NULL)
              ON CONFLICT (provider) DO NOTHING
            SQL
            [@provider, @capacity, now]
          )

          row = @adapter.get_first_row(
            "SELECT tokens, last_refill_at, cooldown_until FROM api_rate_limits WHERE provider = ? FOR UPDATE",
            [@provider]
          )

          if row
            cooldown_until = row["cooldown_until"] ? row["cooldown_until"].to_f : 0.0
            return false if cooldown_until > now

            elapsed   = [now - row["last_refill_at"].to_f, 0.0].max
            available = [row["tokens"].to_f + (elapsed * @refill_rate), @capacity].min
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
      rescue StandardError
        # Lock acquisition failed or serialization error
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

        @adapter.transaction do
          @adapter.execute(
            <<~SQL,
              INSERT INTO api_rate_limits (provider, tokens, last_refill_at, cooldown_until)
              VALUES (?, ?, ?, ?)
              ON CONFLICT (provider) DO UPDATE SET
                cooldown_until = GREATEST(COALESCE(api_rate_limits.cooldown_until, 0), EXCLUDED.cooldown_until)
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
      rescue StandardError
        # Retry once after brief pause
        sleep 0.05
        retry
      end

      # Returns remaining cooldown duration in seconds
      def current_cooldown
        row = @adapter.get_first_row(
          "SELECT cooldown_until FROM api_rate_limits WHERE provider = ?",
          [@provider]
        )
        return 0.0 unless row && row["cooldown_until"]

        [row["cooldown_until"].to_f - Time.now.to_f, 0.0].max
      end

      private

      def ensure_schema!
        @adapter.execute(
          "ALTER TABLE api_rate_limits ADD COLUMN IF NOT EXISTS cooldown_until DOUBLE PRECISION"
        )
      rescue StandardError
        # Table might not exist yet before migrations run
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
