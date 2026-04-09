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
      when nil
        @stderr.puts(usage)
        1
      else
        common_options = parse_common_options!(argv)
        config_path = common_options[:config_path]
        config = Tickrake::ConfigLoader.load(config_path)
        runtime = Tickrake::Runtime.new(config: config, verbose: common_options[:verbose], stdout: @stdout)

        run_command(command, argv, runtime, config_path)
      end
    rescue Tickrake::Error => e
      @stderr.puts(e.message)
      1
    rescue OptionParser::ParseError => e
      @stderr.puts(e.message)
      1
    end

    private

    def run_command(command, argv, runtime, config_path)
      case command
      when "validate-config"
        @stdout.puts("Config valid: #{config_path}")
        0
      when "run"
        run_subcommand(argv, runtime)
      else
        @stderr.puts(usage)
        1
      end
    end

    def run_subcommand(argv, runtime)
      name = argv.shift
      options = parse_run_options!(argv)

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

    def parse_run_options!(argv)
      options = { job: false, from_config_start: false }
      parser = OptionParser.new do |opts|
        opts.on("--job", "Run as a long-lived scheduler job") { options[:job] = true }
        opts.on("--from-config-start", "Always use the configured candle start_date for candle requests") do
          options[:from_config_start] = true
        end
      end
      parser.order!(argv)
      options
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
      log_path = Tickrake::PathSupport.log_path
      FileUtils.mkdir_p(File.dirname(log_path))

      if File.exist?(config_path) && !options[:force]
        raise Tickrake::Error, "Config already exists at #{config_path}. Use --force to overwrite it."
      end

      template_path = File.expand_path("../../config/tickrake.example.yml", __dir__)
      File.write(config_path, File.read(template_path))
      @stdout.puts("Initialized Tickrake home at #{home_dir}")
      @stdout.puts("Config written to #{config_path}")
      @stdout.puts("SQLite DB will be created at #{sqlite_path} on first run")
      @stdout.puts("Log file will be written to #{log_path}")
      0
    end

    def usage
      <<~TEXT
        Usage:
          tickrake init [--config path/to/tickrake.yml] [--force]
          tickrake validate-config [--config path/to/tickrake.yml] [--verbose]
          tickrake run options [--job] [--config path/to/tickrake.yml] [--verbose]
          tickrake run candles [--job] [--from-config-start] [--config path/to/tickrake.yml] [--verbose]
      TEXT
    end
  end
end
