# frozen_string_literal: true

module Tickrake
  class ProviderFactory
    def initialize(config, client_factory: nil)
      @config = config
      @client_factory = client_factory || ClientFactory.new(config)
    end

    def build
      case @config.provider
      when "schwab"
        Providers::Schwab.new(client: @client_factory.build)
      when "ibkr"
        Providers::Ibkr.new(settings: @config.provider_settings)
      else
        raise ConfigError, "Unsupported provider: #{@config.provider}"
      end
    end
  end
end
