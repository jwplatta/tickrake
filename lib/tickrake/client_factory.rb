# frozen_string_literal: true

module Tickrake
  class ClientFactory
    def initialize(config)
      @config = config
    end

    def build
      api_key = ENV.fetch("SCHWAB_API_KEY")
      app_secret = ENV.fetch("SCHWAB_APP_SECRET")
      token_path = ENV["SCHWAB_TOKEN_PATH"] || ENV["TOKEN_PATH"] || SchwabRb::Constants::DEFAULT_TOKEN_PATH
      client = SchwabRb::Auth.init_client_token_file(
        api_key,
        app_secret,
        Tickrake::PathSupport.expand_path(token_path)
      )
      client.refresh!
      client
    rescue KeyError => e
      raise ConfigError, "Missing required environment variable: #{e.key}"
    end
  end
end
