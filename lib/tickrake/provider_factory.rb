# frozen_string_literal: true

module Tickrake
  class ProviderFactory
    def initialize(config, provider_name:, client_factory: nil)
      @config = config
      @provider_name = provider_name
      @client_factory = client_factory || ClientFactory.new(config)
    end

    def build
      provider = @config.provider_definition(@provider_name)

      case provider.adapter
      when "schwab"
        Providers::Schwab.new(provider_name: provider.name, client: @client_factory.build)
      when "ibkr"
        Providers::Ibkr.new(provider_name: provider.name, settings: provider.settings)
      when "massive"
        raise ConfigError, "Provider adapter massive is import-only."
      else
        raise ConfigError, "Unsupported provider adapter: #{provider.adapter}"
      end
    end
  end
end
