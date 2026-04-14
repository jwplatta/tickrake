# frozen_string_literal: true

module Tickrake
  class CLI
    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def call(argv)
      command = argv.shift

      case command
      when "init"
        init_command(argv)
      when "status"
        status_command(argv)
      when "stop"
        stop_command(argv)
      when "restart"
        common_options = parse_common_options!(argv)
        restart_command(argv, common_options)
      when "logs"
        logs_command(argv)
      when nil
        @stderr.puts(usage)
        1
      else
        common_options = parse_common_options!(argv)
        config_path = common_options[:config_path]
        config = Tickrake::ConfigLoader.load(config_path)
        run_command(command, argv, config, common_options, config_path)
      end
    rescue Tickrake::Error => e
      @stderr.puts(e.message)
      1
    rescue OptionParser::ParseError => e
      @stderr.puts(e.message)
      1
    end

    private

    def run_command(command, argv, config, common_options, config_path)
      case command
      when "validate-config"
        @stdout.puts("Config valid: #{config_path}")
        0
      when "start"
        start_subcommand(argv, config_path)
      when "run"
        run_subcommand(argv, config, common_options)
      when "query"
        query_command(argv, config)
      else
        @stderr.puts(usage)
        1
      end
    end

    def run_subcommand(argv, config, common_options)
      name = argv.shift

      case name
      when "options"
        run_options_subcommand(argv, config, common_options)
      when "candles"
        run_candles_subcommand(argv, config, common_options)
      else
        @stderr.puts(usage)
        1
      end
    end

    def run_options_subcommand(argv, config, common_options)
      options = parse_options_run_options!(argv)
      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: runtime_log_path("run", ["options"])
      )

      if options[:job]
        raise Tickrake::Error, "Direct option run arguments cannot be combined with --job." if direct_options_run?(options)

        Tickrake::OptionsMonitorRunner.new(runtime).run
      else
        job = Tickrake::OptionsJob.new(
          runtime,
          universe: direct_options_universe(options),
          expiration_date: options[:expiration_date]
        )
        job.run
        @stdout.puts("Completed one-off options scrape.")
      end
      0
    end

    def run_candles_subcommand(argv, config, common_options)
      options = parse_candles_run_options!(argv)
      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: runtime_log_path("run", ["candles"])
      )

      if options[:job]
        raise Tickrake::Error, "Direct candle run arguments cannot be combined with --job." if direct_candles_run?(options)

        Tickrake::EodCandlesRunner.new(runtime, from_config_start: options[:from_config_start]).run
      else
        job = Tickrake::CandlesJob.new(
          runtime,
          from_config_start: options[:from_config_start],
          universe: direct_candles_universe(options),
          start_date_override: options[:start_date],
          end_date_override: options[:end_date]
        )
        job.run
        @stdout.puts("Completed one-off candle scrape.")
      end
      0
    end

    def start_subcommand(argv, config_path)
      name = argv.shift
      options = parse_job_run_options!(argv)
      starter = Tickrake::BackgroundProcess.new(stdout: @stdout)

      case name
      when "options"
        starter.start(job_name: "options", config_path: config_path, provider_name: options[:provider])
        0
      when "candles"
        starter.start(
          job_name: "candles",
          config_path: config_path,
          from_config_start: options[:from_config_start],
          provider_name: options[:provider]
        )
        0
      else
        @stderr.puts(usage)
        1
      end
    end

    def parse_job_run_options!(argv)
      options = { job: false, from_config_start: false, provider: nil }
      parser = OptionParser.new
      add_job_run_options(parser, options)
      parser.order!(argv)
      options
    end

    def parse_options_run_options!(argv)
      options = { job: false, from_config_start: false, provider: nil, ticker: nil, expiration_date: nil, option_root: nil }

      parser = OptionParser.new do |opts|
        add_job_run_options(opts, options)
        opts.on("--ticker SYMBOL", "Fetch options for a single ticker instead of the configured universe") do |value|
          options[:ticker] = value
        end
        opts.on("--expiration-date YYYY-MM-DD", "Fetch the exact option expiration date") do |value|
          options[:expiration_date] = Date.iso8601(value)
        end
        opts.on("--option-root ROOT", "Filter option results to a single option root") do |value|
          options[:option_root] = value
        end
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      validate_direct_options_run!(options)
      options
    end

    def parse_candles_run_options!(argv)
      options = { job: false, from_config_start: false, provider: nil, ticker: nil, start_date: nil, end_date: nil, frequency: nil }

      parser = OptionParser.new do |opts|
        add_job_run_options(opts, options)
        opts.on("--ticker SYMBOL", "Fetch candles for a single ticker instead of the configured universe") do |value|
          options[:ticker] = value
        end
        opts.on("--start-date YYYY-MM-DD", "Fetch candles starting from this date") do |value|
          options[:start_date] = Date.iso8601(value)
        end
        opts.on("--end-date YYYY-MM-DD", "Fetch candles through this date") do |value|
          options[:end_date] = Date.iso8601(value)
        end
        opts.on("--frequency FREQ", "Fetch a single candle frequency") do |value|
          options[:frequency] = Tickrake::Query::FrequencyNormalizer.new.normalize(value)
        end
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      validate_direct_candles_run!(options)
      options
    end

    def add_job_run_options(parser, options)
      parser.on("--job", "Run as a long-lived scheduler job") { options[:job] = true }
      parser.on("--provider NAME", "Use the named provider from config") { |value| options[:provider] = value }
      parser.on("--from-config-start", "Always use the configured candle start_date for candle requests") do
        options[:from_config_start] = true
      end
    end

    def validate_direct_options_run!(options)
      return unless options[:ticker] || options[:expiration_date] || options[:option_root]

      raise Tickrake::Error, "Direct option runs require --ticker." unless options[:ticker]
      raise Tickrake::Error, "Direct option runs require --expiration-date." unless options[:expiration_date]
    end

    def validate_direct_candles_run!(options)
      direct_values = [options[:ticker], options[:start_date], options[:end_date], options[:frequency]]
      return unless direct_values.any?

      raise Tickrake::Error, "Direct candle runs require --ticker." unless options[:ticker]
      raise Tickrake::Error, "Direct candle runs require --start-date." unless options[:start_date]
      raise Tickrake::Error, "Direct candle runs require --end-date." unless options[:end_date]
      raise Tickrake::Error, "Direct candle runs require --frequency." unless options[:frequency]
      raise Tickrake::Error, "--from-config-start cannot be combined with direct candle run arguments." if options[:from_config_start]
      raise Tickrake::Error, "--end-date must be on or after --start-date." if options[:end_date] < options[:start_date]
    end

    def direct_options_run?(options)
      !!options[:ticker]
    end

    def direct_candles_run?(options)
      !!options[:ticker]
    end

    def direct_options_universe(options)
      return nil unless direct_options_run?(options)

      [Tickrake::OptionSymbol.new(symbol: options[:ticker], option_root: options[:option_root])]
    end

    def direct_candles_universe(options)
      return nil unless direct_candles_run?(options)

      [Tickrake::CandleSymbol.new(
        symbol: options[:ticker],
        frequencies: [options[:frequency]],
        start_date: options[:start_date],
        need_extended_hours_data: false,
        need_previous_close: false
      )]
    end

    def query_command(argv, config)
      options = parse_query_options!(argv)
      tracker = Tickrake::Tracker.new(config.sqlite_path)
      Tickrake::Query::Engine.new(config: config, tracker: tracker, stdout: @stdout).run(
        type: options[:type],
        provider_name: options[:provider],
        ticker: options[:ticker],
        frequency: options[:frequency],
        start_date: options[:start_date],
        end_date: options[:end_date],
        format: options[:format]
      )
      0
    end

    def parse_query_options!(argv)
      options = {
        type: nil,
        provider: nil,
        ticker: nil,
        frequency: nil,
        start_date: nil,
        end_date: nil,
        format: "text"
      }
      parser = OptionParser.new do |opts|
        opts.on("--type TYPE", "Dataset type: candles or options") { |value| options[:type] = value }
        opts.on("--provider NAME", "Use the named provider namespace from config") { |value| options[:provider] = value }
        opts.on("--ticker SYMBOL", "Filter by ticker symbol") { |value| options[:ticker] = value }
        opts.on("--frequency FREQ", "Filter candle results by frequency") { |value| options[:frequency] = value }
        opts.on("--start-date YYYY-MM-DD", "Filter by dataset coverage start date") do |value|
          options[:start_date] = Date.iso8601(value)
        end
        opts.on("--end-date YYYY-MM-DD", "Filter by dataset coverage end date") do |value|
          options[:end_date] = Date.iso8601(value)
        end
        opts.on("--format FORMAT", "Output format: text or json") { |value| options[:format] = value }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      options
    end

    def status_command(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      registry = Tickrake::JobRegistry.new
      registry.statuses.each do |job|
        case job[:state]
        when "running"
          @stdout.puts("#{job[:name]}: running pid=#{job[:pid]} started_at=#{job[:started_at]} log=#{job[:log_path]}")
        when "stale"
          @stdout.puts("#{job[:name]}: stale pid=#{job[:pid]} started_at=#{job[:started_at]}")
        else
          @stdout.puts("#{job[:name]}: stopped")
        end
      end
      0
    end

    def stop_command(argv)
      target = argv.shift
      raise OptionParser::MissingArgument, "job name" if target.nil?
      raise OptionParser::InvalidOption, argv.first if argv.any?

      registry = Tickrake::JobRegistry.new
      targets = resolve_job_targets(target)

      targets.each do |name|
        stop_one(registry, name)
      end
      0
    end

    def restart_command(argv, common_options)
      target = argv.shift
      raise OptionParser::MissingArgument, "job name" if target.nil?

      options = parse_job_run_options!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      registry = Tickrake::JobRegistry.new
      starter = Tickrake::BackgroundProcess.new(stdout: @stdout)
      targets = resolve_job_targets(target)

      targets.each do |name|
        metadata = registry.read(name) || {}
        stop_one(registry, name, timeout_seconds: nil, waiting_message: restart_waiting_message(name))
        starter.start(
          job_name: name,
          config_path: restart_config_path(common_options, metadata),
          from_config_start: restart_from_config_start(name, options, metadata),
          provider_name: restart_provider_name(options, metadata)
        )
      end
      0
    end

    def stop_one(registry, name, timeout_seconds: 5, waiting_message: nil)
      job = registry.status(name)
      case job[:state]
      when "running"
        Process.kill("TERM", Integer(job[:pid]))
        wait_for_stop(
          registry,
          name,
          Integer(job[:pid]),
          timeout_seconds: timeout_seconds,
          waiting_message: waiting_message
        )
      when "stale"
        registry.delete(name)
        @stdout.puts("Removed stale #{name} job metadata for pid #{job[:pid]}.")
      else
        @stdout.puts("#{name} job is not running.")
      end
    end

    def wait_for_stop(registry, name, pid, timeout_seconds: 5, waiting_message: nil)
      deadline = timeout_seconds && (Time.now + timeout_seconds)
      @stdout.puts(waiting_message) if waiting_message
      loop do
        unless registry.pid_alive?(pid)
          registry.delete(name)
          @stdout.puts("Stopped #{name} job (pid #{pid}).")
          return
        end

        break if deadline && Time.now >= deadline

        sleep 0.2
      end

      @stdout.puts("Sent TERM to #{name} job (pid #{pid}); waiting for shutdown.")
    end

    def restart_waiting_message(name)
      "Waiting for #{name} job to finish its current work before restarting. This can take a bit."
    end

    def resolve_job_targets(target)
      targets = target == "all" ? Tickrake::JobRegistry::JOB_NAMES : [target]
      invalid = targets.reject { |name| Tickrake::JobRegistry::JOB_NAMES.include?(name) }
      raise Tickrake::Error, "Unknown job `#{invalid.first}`." if invalid.any?

      targets
    end

    def restart_config_path(common_options, metadata)
      explicit = common_options[:config_path]
      default = Tickrake::PathSupport.config_path
      return explicit if explicit != default

      metadata[:config_path] || explicit
    end

    def restart_provider_name(options, metadata)
      options[:provider] || metadata[:provider_name]
    end

    def restart_from_config_start(name, options, metadata)
      return false unless name == "candles"
      return true if options[:from_config_start]

      metadata[:from_config_start] == true
    end

    def logs_command(argv)
      options = parse_logs_options!(argv)
      log_path = Tickrake::PathSupport.named_log_path(options[:target])
      unless File.exist?(log_path)
        @stdout.puts("No log file at #{log_path}")
        return 0
      end

      content = File.read(log_path)
      if options[:tail]
        @stdout.print(content.lines.last(options[:tail]).join)
      else
        @stdout.print(content)
      end
      0
    end

    def parse_logs_options!(argv)
      options = { tail: nil, target: "cli" }
      if argv.first && !argv.first.start_with?("-")
        options[:target] = argv.shift
      end
      parser = OptionParser.new do |opts|
        opts.on("--tail N", Integer, "Show only the last N log lines") { |value| options[:tail] = value }
      end
      parser.order!(argv)
      if argv.first && !argv.first.start_with?("-")
        options[:target] = argv.shift
      end
      raise OptionParser::InvalidOption, argv.first if argv.any?
      options
    end

    def runtime_log_path(command, argv)
      return Tickrake::PathSupport.cli_log_path unless command == "run"

      case argv.first
      when "options"
        Tickrake::PathSupport.options_log_path
      when "candles"
        Tickrake::PathSupport.candles_log_path
      else
        Tickrake::PathSupport.cli_log_path
      end
    end

    def parse_common_options!(argv)
      options = { config_path: Tickrake::PathSupport.config_path, verbose: false }

      index = 0
      while index < argv.length
        case argv[index]
        when "--config"
          value = argv[index + 1]
          raise OptionParser::MissingArgument, "--config" if value.nil?

          options[:config_path] = value
          argv.slice!(index, 2)
        when /\A--config=(.+)\z/
          options[:config_path] = Regexp.last_match(1)
          argv.delete_at(index)
        when "--verbose"
          options[:verbose] = true
          argv.delete_at(index)
        else
          index += 1
        end
      end
      options
    end

    def init_command(argv)
      options = { force: false, config_path: Tickrake::PathSupport.config_path }
      parser = OptionParser.new do |opts|
        opts.on("--config PATH", "Where to write the Tickrake config") { |value| options[:config_path] = value }
        opts.on("--force", "Overwrite an existing config file") { options[:force] = true }
      end
      parser.order!(argv)

      config_path = Tickrake::PathSupport.expand_path(options[:config_path])
      home_dir = Tickrake::PathSupport.home_dir
      FileUtils.mkdir_p(home_dir)
      FileUtils.mkdir_p(File.dirname(config_path))
      sqlite_path = Tickrake::PathSupport.sqlite_path
      FileUtils.mkdir_p(File.dirname(sqlite_path))
      log_path = Tickrake::PathSupport.cli_log_path
      FileUtils.mkdir_p(File.dirname(log_path))
      FileUtils.mkdir_p(File.dirname(Tickrake::PathSupport.options_log_path))
      FileUtils.mkdir_p(File.dirname(Tickrake::PathSupport.candles_log_path))

      if File.exist?(config_path) && !options[:force]
        raise Tickrake::Error, "Config already exists at #{config_path}. Use --force to overwrite it."
      end

      template_path = File.expand_path("../../config/tickrake.example.yml", __dir__)
      File.write(config_path, File.read(template_path))
      @stdout.puts("Initialized Tickrake home at #{home_dir}")
      @stdout.puts("Config written to #{config_path}")
      @stdout.puts("SQLite DB will be created at #{sqlite_path} on first run")
      @stdout.puts("CLI log file will be written to #{log_path}")
      @stdout.puts("Options job log file will be written to #{Tickrake::PathSupport.options_log_path}")
      @stdout.puts("Candles job log file will be written to #{Tickrake::PathSupport.candles_log_path}")
      0
    end

    def usage
      <<~TEXT
        Usage:
          tickrake init [--config path/to/tickrake.yml] [--force]
          tickrake validate-config [--config path/to/tickrake.yml] [--verbose]
          tickrake start options [--provider NAME] [--config path/to/tickrake.yml]
          tickrake start candles [--provider NAME] [--from-config-start] [--config path/to/tickrake.yml]
          tickrake restart options [--provider NAME]
          tickrake restart candles [--provider NAME] [--from-config-start]
          tickrake restart all [--provider NAME] [--from-config-start]
          tickrake run options [--job] [--provider NAME] [--ticker SYMBOL --expiration-date YYYY-MM-DD [--option-root ROOT]] [--config path/to/tickrake.yml] [--verbose]
          tickrake run candles [--job] [--provider NAME] [--from-config-start] [--ticker SYMBOL --start-date YYYY-MM-DD --end-date YYYY-MM-DD --frequency FREQ] [--config path/to/tickrake.yml] [--verbose]
          tickrake query [--type candles|options] [--provider NAME] [--ticker SYMBOL] [--frequency FREQ] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--format text|json] [--config path/to/tickrake.yml]
          tickrake status
          tickrake stop options|candles|all
          tickrake logs [cli|options|candles] [--tail N]
      TEXT
    end
  end
end
