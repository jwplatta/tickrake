# frozen_string_literal: true

module Tickrake
  class CLI
    def initialize(stdout: $stdout, stderr: $stderr)
      @stdout = stdout
      @stderr = stderr
    end

    def call(argv)
      command = argv.shift
      config_path = parse_config!(argv)
      config = Tickrake::ConfigLoader.load(config_path)
      runtime = Tickrake::Runtime.new(config: config)

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
    rescue Tickrake::Error => e
      @stderr.puts(e.message)
      1
    rescue OptionParser::ParseError => e
      @stderr.puts(e.message)
      1
    end

    private

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
      config_path = "config/tickrake.example.yml"
      parser = OptionParser.new do |opts|
        opts.on("--config PATH", "Path to Tickrake config") { |value| config_path = value }
      end
      parser.order!(argv)
      config_path
    end

    def usage
      <<~TEXT
        Usage:
          tickrake validate-config --config path/to/tickrake.yml
          tickrake run options-monitor --config path/to/tickrake.yml
          tickrake run eod-candles --config path/to/tickrake.yml
      TEXT
    end
  end
end
