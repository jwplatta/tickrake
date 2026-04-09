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
        config_path = parse_config!(argv)
        config = Tickrake::ConfigLoader.load(config_path)
        runtime = Tickrake::Runtime.new(config: config)

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
        run_subcommand(argv.shift, runtime)
      else
        @stderr.puts(usage)
        1
      end
    end

    def run_subcommand(name, runtime)
      case name
      when "options-monitor"
        Tickrake::OptionsMonitorRunner.new(runtime).run
        0
      when "eod-candles"
        Tickrake::EodCandlesRunner.new(runtime).run
        0
      else
        @stderr.puts(usage)
        1
      end
    end

    def parse_config!(argv)
      config_path = Tickrake::PathSupport.config_path
      parser = OptionParser.new do |opts|
        opts.on("--config PATH", "Path to Tickrake config") { |value| config_path = value }
      end
      parser.order!(argv)
      config_path
    end

    def init_command(argv)
      options = { force: false, config_path: Tickrake::PathSupport.config_path }
      parser = OptionParser.new do |opts|
        opts.on("--config PATH", "Where to write the Tickrake config") { |value| options[:config_path] = value }
        opts.on("--force", "Overwrite an existing config file") { options[:force] = true }
      end
      parser.order!(argv)

      config_path = Tickrake::PathSupport.expand_path(options[:config_path])
      Tickrake::PathSupport.expand_path(Tickrake::PathSupport.home_dir)
      FileUtils.mkdir_p(File.dirname(config_path))
      sqlite_path = Tickrake::PathSupport.sqlite_path
      FileUtils.mkdir_p(File.dirname(sqlite_path))

      if File.exist?(config_path) && !options[:force]
        raise Tickrake::Error, "Config already exists at #{config_path}. Use --force to overwrite it."
      end

      template_path = File.expand_path("../../config/tickrake.example.yml", __dir__)
      File.write(config_path, File.read(template_path))
      @stdout.puts("Initialized Tickrake home at #{Tickrake::PathSupport.home_dir}")
      @stdout.puts("Config written to #{config_path}")
      @stdout.puts("SQLite DB will be created at #{sqlite_path} on first run")
      0
    end

    def usage
      <<~TEXT
        Usage:
          tickrake init [--config path/to/tickrake.yml] [--force]
          tickrake validate-config --config path/to/tickrake.yml
          tickrake run options-monitor --config path/to/tickrake.yml
          tickrake run eod-candles --config path/to/tickrake.yml
      TEXT
    end
  end
end
