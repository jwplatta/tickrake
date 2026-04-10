# frozen_string_literal: true

module Tickrake
  class Runtime
    attr_reader :config, :tracker, :client_factory, :logger

    def initialize(config:, tracker: nil, client_factory: nil, logger: nil, verbose: false, stdout: $stdout, log_path: Tickrake::PathSupport.cli_log_path)
      @config = config
      @tracker = tracker || Tracker.new(config.sqlite_path)
      @client_factory = client_factory || ClientFactory.new(config)
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
