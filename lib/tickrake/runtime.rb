# frozen_string_literal: true

module Tickrake
  class Runtime
    attr_reader :config, :tracker, :client_factory, :provider_factory, :logger, :provider_name, :provider_definition, :provider_override_name, :config_path

    def initialize(config:, tracker: nil, client_factory: nil, provider_factory: nil, logger: nil, provider_name: nil, verbose: false, stdout: $stdout, log_path: Tickrake::PathSupport.cli_log_path, config_path: Tickrake::PathSupport.config_path, context: {})
      @config = config
      @config_path = Tickrake::PathSupport.expand_path(config_path)
      @tracker = tracker || Tracker.new(config)
      @provider_override_name = provider_name
      @provider_name = provider_name || config.default_provider_name
      @provider_definition = config.provider_definition(@provider_name)
      @logger = logger || LoggerFactory.build(verbose: verbose, stdout: stdout, log_path: log_path, context: context)
      @logger.level = Logger::INFO
      @client_factory = client_factory || ClientFactory.new(
        config,
        logger: @logger,
        rate_limiter: (config.respond_to?(:providers) && config.providers.key?("schwab") ? rate_limiter_for("schwab") : nil)
      )
      @provider_factory = provider_factory || ProviderFactory.new(config, provider_name: @provider_name, client_factory: @client_factory)
    end

    def rate_limiter_for(provider_name = nil)
      name = (provider_name || @provider_name).to_s
      return nil unless config.respond_to?(:providers) && config.providers.key?(name)

      @rate_limiters ||= {}
      @rate_limiters[name] ||= begin
        p_def = config.provider_definition(name)
        max_req = p_def.rate_limit_max_requests
        interval = p_def.rate_limit_interval_seconds
        if max_req && interval
          Tickrake::DB::RateLimiter.build(
            config,
            provider: name,
            capacity: max_req,
            refill_rate: max_req.to_f / interval,
            adapter: @tracker&.adapter,
            logger: @logger
          )
        end
      end
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
