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
      else
        @stderr.puts(usage)
        1
      end
    end

    def run_subcommand(argv, config, common_options)
      name = argv.shift
      options = parse_run_options!(argv)
      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: runtime_log_path("run", [name])
      )

      case name
      when "options"
        if options[:job]
          Tickrake::OptionsMonitorRunner.new(runtime).run
        else
          Tickrake::OptionsJob.new(runtime).run
          @stdout.puts("Completed one-off options scrape.")
        end
        0
      when "candles"
        if options[:job]
          Tickrake::EodCandlesRunner.new(runtime, from_config_start: options[:from_config_start]).run
        else
          Tickrake::CandlesJob.new(runtime, from_config_start: options[:from_config_start]).run
          @stdout.puts("Completed one-off candle scrape.")
        end
        0
      else
        @stderr.puts(usage)
        1
      end
    end

    def start_subcommand(argv, config_path)
      name = argv.shift
      options = parse_run_options!(argv)
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

    def parse_run_options!(argv)
      options = { job: false, from_config_start: false, provider: nil }
      parser = OptionParser.new do |opts|
        opts.on("--job", "Run as a long-lived scheduler job") { options[:job] = true }
        opts.on("--provider NAME", "Use the named provider from config") { |value| options[:provider] = value }
        opts.on("--from-config-start", "Always use the configured candle start_date for candle requests") do
          options[:from_config_start] = true
        end
      end
      parser.order!(argv)
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
      targets = target == "all" ? Tickrake::JobRegistry::JOB_NAMES : [target]

      targets.each do |name|
        unless Tickrake::JobRegistry::JOB_NAMES.include?(name)
          raise Tickrake::Error, "Unknown job `#{name}`."
        end

        stop_one(registry, name)
      end
      0
    end

    def stop_one(registry, name)
      job = registry.status(name)
      case job[:state]
      when "running"
        Process.kill("TERM", Integer(job[:pid]))
        wait_for_stop(registry, name, Integer(job[:pid]))
      when "stale"
        registry.delete(name)
        @stdout.puts("Removed stale #{name} job metadata for pid #{job[:pid]}.")
      else
        @stdout.puts("#{name} job is not running.")
      end
    end

    def wait_for_stop(registry, name, pid)
      deadline = Time.now + 5
      while Time.now < deadline
        unless registry.pid_alive?(pid)
          registry.delete(name)
          @stdout.puts("Stopped #{name} job (pid #{pid}).")
          return
        end

        sleep 0.2
      end

      @stdout.puts("Sent TERM to #{name} job (pid #{pid}); waiting for shutdown.")
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
          tickrake run options [--job] [--provider NAME] [--config path/to/tickrake.yml] [--verbose]
          tickrake run candles [--job] [--provider NAME] [--from-config-start] [--config path/to/tickrake.yml] [--verbose]
          tickrake status
          tickrake stop options|candles|all
          tickrake logs [cli|options|candles] [--tail N]
      TEXT
    end
  end
end
