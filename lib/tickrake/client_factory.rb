# frozen_string_literal: true

module Tickrake
  class ClientFactory
    def initialize(config)
      @config = config
    end

    def build
      api_key = ENV.fetch("SCHWAB_API_KEY")
      app_secret = ENV.fetch("SCHWAB_APP_SECRET")
      db_path = ENV["SCHWAB_DATABASE_PATH"] || SchwabRb.configuration.database_path
      database = SchwabRb::Storage::Database.new(db_path)
      client = SchwabRb::Auth.init_client_from_database(api_key, app_secret, database: database)
      client.refresh!
      client
    rescue KeyError => e
      raise ConfigError, "Missing required environment variable: #{e.key}"
    end
  end
end
