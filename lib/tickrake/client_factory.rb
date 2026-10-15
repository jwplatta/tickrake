# frozen_string_literal: true

module Tickrake
  class ClientFactory
    attr_reader :config, :logger, :rate_limiter

    def initialize(config, logger: nil, rate_limiter: nil)
      @config = config
      @logger = logger
      @rate_limiter = rate_limiter
    end

    def build
      api_key = ENV.fetch("SCHWAB_API_KEY")
      app_secret = ENV.fetch("SCHWAB_APP_SECRET")
      db_path = ENV["SCHWAB_DATABASE_PATH"] || SchwabRb.configuration.database_path
      @logger&.info({ msg: "Opening Schwab database", event: "schwab_db_open", path: db_path })
      database = SchwabRb::Storage::Database.new(db_path)
      client = SchwabRb::Auth.init_client_from_database(api_key, app_secret, database: database)
      client.refresh!

      client.diagnostic_logger = @logger if client.respond_to?(:diagnostic_logger=)
      client.rate_limiter = (@rate_limiter || default_rate_limiter) if client.respond_to?(:rate_limiter=)

      client
    rescue KeyError => e
      raise ConfigError, "Missing required environment variable: #{e.key}"
    end

    private

    def default_rate_limiter
      return nil unless @config

      p_def = @config.provider_definition("schwab")
      max_req = p_def.rate_limit_max_requests
      interval = p_def.rate_limit_interval_seconds
      if max_req && interval
        Tickrake::DB::RateLimiter.build(
          @config,
          provider: "schwab",
          capacity: max_req,
          refill_rate: max_req.to_f / interval,
          logger: @logger
        )
      end
    end
  end
end
