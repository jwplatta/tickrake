# frozen_string_literal: true

module Tickrake
  class Error < StandardError; end
  class ConfigError < Error; end
  class LockError < Error; end
  class SchedulerRestartRequired < Error
    EXIT_STATUS = 75

    attr_reader :provider_name, :job_name, :failure_count, :threshold, :cooldown_seconds

    def initialize(provider_name:, job_name:, failure_count:, threshold:, cooldown_seconds:)
      @provider_name = provider_name
      @job_name = job_name
      @failure_count = failure_count
      @threshold = threshold
      @cooldown_seconds = cooldown_seconds
      super(
        "Scheduler #{job_name} reached #{failure_count} consecutive #{provider_name} failures " \
        "(threshold=#{threshold}); exiting for supervised restart."
      )
    end

    def exit_status
      EXIT_STATUS
    end
  end

  class RateLimitWaitTimeoutError < Error
    attr_reader :provider, :elapsed_seconds, :timeout_seconds

    def initialize(provider:, elapsed_seconds:, timeout_seconds: 120.0, message: nil)
      @provider = provider
      @elapsed_seconds = elapsed_seconds
      @timeout_seconds = timeout_seconds
      msg = message || "Rate limit wait timeout for provider #{@provider} after #{@elapsed_seconds.round(2)}s (timeout: #{@timeout_seconds}s)"
      super(msg)
    end
  end

  class ResponseParseError < JSON::ParserError
    attr_reader :response_metadata, :correlation_id, :http_status, :content_type, :content_length, :body_bytes, :duration_ms

    def initialize(message, response_metadata: nil)
      super(message)
      @response_metadata = response_metadata || {}
      @correlation_id = @response_metadata[:correlation_id]
      @http_status = @response_metadata[:http_status]
      @content_type = @response_metadata[:content_type]
      @content_length = @response_metadata[:content_length]
      @body_bytes = @response_metadata[:body_bytes]
      @duration_ms = @response_metadata[:duration_ms]
    end
  end
end
