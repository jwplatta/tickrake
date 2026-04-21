# frozen_string_literal: true

module Tickrake
  class Runtime
    attr_reader :config, :tracker, :client_factory, :provider_factory, :logger, :provider_name, :provider_definition, :provider_override_name, :config_path

    def initialize(config:, tracker: nil, client_factory: nil, provider_factory: nil, logger: nil, provider_name: nil, verbose: false, stdout: $stdout, log_path: Tickrake::PathSupport.cli_log_path, config_path: Tickrake::PathSupport.config_path)
      @config = config
      @config_path = Tickrake::PathSupport.expand_path(config_path)
      @tracker = tracker || Tracker.new(config.sqlite_path)
      @provider_override_name = provider_name
      @provider_name = provider_name || config.default_provider_name
      @provider_definition = config.provider_definition(@provider_name)
      @client_factory = client_factory || ClientFactory.new(config)
      @provider_factory = provider_factory || ProviderFactory.new(config, provider_name: @provider_name, client_factory: @client_factory)
      @logger = logger || LoggerFactory.build(verbose: verbose, stdout: stdout, log_path: log_path)
      @logger.level = Logger::INFO
    end

    def with_timezone
      previous = ENV["TZ"]
      ENV["TZ"] = config.timezone if config.timezone
      yield
    ensure
      ENV["TZ"] = previous
    end
  end
end
