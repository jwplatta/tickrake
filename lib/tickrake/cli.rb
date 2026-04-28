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
      when "logs"
        logs_command(argv)
      when nil
        @stderr.puts(usage)
        1
      else
        common_options = parse_common_options!(argv)
        dispatch(command, argv, common_options)
      end
    rescue Tickrake::Error => e
      @stderr.puts(e.message)
      1
    rescue OptionParser::ParseError => e
      @stderr.puts(e.message)
      1
    end

    private

    def dispatch(command, argv, common_options)
      config_path = common_options[:config_path]

      case command
      when "validate-config"
        validate_config_command(argv, config_path)
      when "import"
        config = Tickrake::ConfigLoader.load(config_path)
        import_command(argv, config, common_options)
      when "storage-stats"
        config = Tickrake::ConfigLoader.load(config_path)
        storage_stats_command(argv, config)
      when "query"
        config = Tickrake::ConfigLoader.load(config_path)
        query_command(argv, config)
      when "status"
        config = Tickrake::ConfigLoader.load(config_path)
        status_command(argv, config)
      when "start"
        start_command(argv, config_path)
      when "stop"
        stop_command(argv, config_path)
      when "restart"
        restart_command(argv, config_path)
      when "run"
        config = Tickrake::ConfigLoader.load(config_path)
        run_command(argv, config, common_options)
      else
        @stderr.puts(usage)
        1
      end
    end

    def validate_config_command(argv, config_path)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      Tickrake::ConfigLoader.load(config_path)
      @stdout.puts("Config valid: #{config_path}")
      0
    end

    def import_command(argv, config, common_options)
      options = parse_import_options!(argv)
      if options[:job]
        import_configured_job(config, common_options, options)
      else
        import_direct(config, common_options, options)
      end
    end

    def import_configured_job(config, common_options, options)
      job = config.import_job(options[:job])
      validate_import_job_options!(options)

      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: job.provider,
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path(job.name),
        config_path: common_options[:config_path]
      )

      results = import_paths(
        type: job.type,
        config: config,
        runtime: runtime,
        provider_name: job.provider,
        ticker: job.ticker,
        option_root: job.option_root,
        paths: job.paths,
        force: options[:force] || job.force,
        progress_reporter: build_import_progress_reporter(job.paths)
      )
      @stdout.puts("Imported #{results.sum(&:row_count)} option rows into #{results.length} snapshot files from #{job.paths.length} source files.")
      0
    end

    def import_direct(config, common_options, options)
      validate_import_options!(options)

      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path("import"),
        config_path: common_options[:config_path]
      )

      import_paths = [options[:path]]
      results = import_paths(
        type: options[:type],
        config: config,
        runtime: runtime,
        provider_name: options[:provider],
        ticker: options[:ticker],
        option_root: options[:option_root],
        paths: import_paths,
        force: options[:force],
        progress_reporter: build_import_progress_reporter(import_paths)
      )
      @stdout.puts("Imported #{results.sum(&:row_count)} option rows into #{results.length} snapshot files.")
      0
    end

    def import_paths(type:, config:, runtime:, provider_name:, ticker:, option_root:, paths:, force:, progress_reporter:)
      begin
        case type
        when "options"
          paths.flat_map do |path|
            begin
              Tickrake::Importers::MassiveOptionsImporter.new(
                config: config,
                tracker: runtime.tracker,
                provider_name: provider_name,
                ticker: ticker,
                option_root: option_root,
                source_path: path,
                force: force,
                logger: runtime.logger
              ).import.tap do
                progress_reporter&.advance(title: import_progress_title(path))
              end
            rescue StandardError
              progress_reporter&.advance(title: "#{import_progress_title(path)} failed")
              raise
            end
          end
        else
          raise Tickrake::Error, "Unsupported import type `#{type}`."
        end
      ensure
        progress_reporter&.finish
      end
    end

    def build_import_progress_reporter(paths)
      Tickrake::ProgressReporter.build(total: paths.length, title: "Import", output: @stdout)
    end

    def import_progress_title(path)
      "Import #{File.basename(path)}"
    end

    def run_command(argv, config, common_options)
      options = parse_run_options!(argv)

      if options[:job]
        run_configured_job(config, common_options, options)
      else
        run_direct_job(config, common_options, options)
      end
    end

    def run_configured_job(config, common_options, options)
      job = config.job(options[:job])
      validate_job_run_options!(job, options)

      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path(job.name),
        config_path: common_options[:config_path]
      )

      if options[:scheduler]
        run_scheduler(runtime, job, from_config_start: options[:from_config_start])
      elsif options[:supervisor]
        run_supervisor(runtime, job, from_config_start: options[:from_config_start])
      else
        run_job_once(runtime, job, from_config_start: options[:from_config_start])
      end
      0
    end

    def run_direct_job(config, common_options, options)
      validate_direct_run_options!(options)

      runtime = Tickrake::Runtime.new(
        config: config,
        provider_name: options[:provider],
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path(options[:type]),
        config_path: common_options[:config_path]
      )

      case options[:type]
      when "options"
        job = Tickrake::OptionsJob.new(
          runtime,
          universe: direct_options_universe(options),
          expiration_date: options[:expiration_date],
          progress_reporter: nil
        )
        job.run
        @stdout.puts("Completed one-off options scrape.")
      when "candles"
        job = Tickrake::CandlesJob.new(
          runtime,
          from_config_start: false,
          universe: direct_candles_universe(options),
          start_date_override: options[:start_date],
          end_date_override: options[:end_date],
          progress_output: @stdout
        )
        job.run
        @stdout.puts("Completed one-off candle scrape.")
      else
        raise Tickrake::Error, "Unknown run type `#{options[:type]}`."
      end
      0
    end

    def run_job_once(runtime, job, from_config_start:)
      case job.type
      when "options"
        progress_reporter = Tickrake::ProgressReporter.build(
          total: job.universe.length * job.dte_buckets.uniq.length,
          title: "Options",
          output: @stdout
        )
        Tickrake::OptionsJob.new(runtime, progress_reporter: progress_reporter, scheduled_job: job).run
        @stdout.puts("Completed job #{job.name}.")
      when "candles"
        Tickrake::CandlesJob.new(
          runtime,
          from_config_start: from_config_start,
          progress_output: @stdout,
          scheduled_job: job
        ).run
        @stdout.puts("Completed job #{job.name}.")
      else
        raise Tickrake::Error, "Unknown job type `#{job.type}`."
      end
    end

    def run_scheduler(runtime, job, from_config_start:)
      case job.type
      when "options"
        Tickrake::OptionsMonitorRunner.new(runtime, scheduled_job: job).run
      when "candles"
        Tickrake::CandlesSchedulerRunner.new(runtime, scheduled_job: job, from_config_start: from_config_start).run
      else
        raise Tickrake::Error, "Unknown job type `#{job.type}`."
      end
    end

    def run_supervisor(runtime, job, from_config_start:)
      Tickrake::SchedulerSupervisor.new(
        runtime,
        scheduled_job: job,
        from_config_start: from_config_start
      ).run
    end

    def start_command(argv, config_path)
      options = parse_job_control_options!(argv, restart_default: false)
      Tickrake::JobControl.new(stdout: @stdout).start(
        target: options[:job],
        config_path: config_path,
        provider_name: options[:provider],
        from_config_start: options[:from_config_start],
        restart: options[:restart]
      )
      0
    end

    def stop_command(argv, config_path)
      options = parse_stop_options!(argv)
      Tickrake::JobControl.new(stdout: @stdout).stop(target: options[:job], config_path: config_path)
      0
    end

    def restart_command(argv, config_path)
      options = parse_job_control_options!(argv, restart_default: nil)
      Tickrake::JobControl.new(stdout: @stdout).restart(
        target: options[:job],
        config_path: config_path,
        provider_name: options[:provider],
        from_config_start: options[:from_config_start],
        restart: options[:restart]
      )
      0
    end

    def parse_job_control_options!(argv, restart_default:)
      options = { job: nil, provider: nil, from_config_start: false, restart: restart_default }
      parser = OptionParser.new do |opts|
        opts.on("--job NAME", "Configured job name or all") { |value| options[:job] = value }
        opts.on("--provider NAME", "Use the named provider from config") { |value| options[:provider] = value }
        opts.on("--from-config-start", "For candles jobs, backfill from configured start_date") { options[:from_config_start] = true }
        opts.on("--restart", "Restart the background scheduler automatically if it exits unexpectedly") do
          options[:restart] = true
        end
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?
      raise Tickrake::Error, "--job is required." unless options[:job]

      options
    end

    def parse_stop_options!(argv)
      options = { job: nil }
      parser = OptionParser.new do |opts|
        opts.on("--job NAME", "Configured job name or all") { |value| options[:job] = value }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?
      raise Tickrake::Error, "--job is required." unless options[:job]

      options
    end

    def parse_run_options!(argv)
      options = {
        type: nil,
        job: nil,
        scheduler: false,
        provider: nil,
        from_config_start: false,
        supervisor: false,
        ticker: nil,
        expiration_date: nil,
        option_root: nil,
        start_date: nil,
        end_date: nil,
        frequency: nil
      }

      parser = OptionParser.new do |opts|
        opts.on("--type TYPE", "Run type for ad hoc runs: candles or options") { |value| options[:type] = value }
        opts.on("--job NAME", "Configured job name to run") { |value| options[:job] = value }
        opts.on("--provider NAME", "Use the named provider from config") { |value| options[:provider] = value }
        opts.on("--from-config-start", "For candles jobs, backfill from configured start_date") { options[:from_config_start] = true }
        opts.on("--ticker SYMBOL", "Fetch a single ticker") { |value| options[:ticker] = value }
        opts.on("--expiration-date YYYY-MM-DD", "Fetch the exact option expiration date") do |value|
          options[:expiration_date] = Date.iso8601(value)
        end
        opts.on("--option-root ROOT", "Filter option results to a single option root") { |value| options[:option_root] = value }
        opts.on("--start-date YYYY-MM-DD", "Fetch candles starting from this date") do |value|
          options[:start_date] = Date.iso8601(value)
        end
        opts.on("--end-date YYYY-MM-DD", "Fetch candles through this date") do |value|
          options[:end_date] = Date.iso8601(value)
        end
        opts.on("--frequency FREQ", "Fetch a single candle frequency") do |value|
          options[:frequency] = Tickrake::Query::FrequencyNormalizer.new.normalize(value)
        end
        opts.on("--scheduler", "Internal: run the configured scheduler loop") { options[:scheduler] = true }
        opts.on("--supervisor", "Internal: supervise and restart the configured scheduler loop") { options[:supervisor] = true }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      options
    end

    def parse_import_options!(argv)
      options = {
        type: nil,
        job: nil,
        provider: nil,
        ticker: nil,
        option_root: nil,
        path: nil,
        force: false
      }

      parser = OptionParser.new do |opts|
        opts.on("--type TYPE", "Import type: options") { |value| options[:type] = value }
        opts.on("--job NAME", "Configured import job name to run") { |value| options[:job] = value }
        opts.on("--provider NAME", "Use the named provider from config") { |value| options[:provider] = value }
        opts.on("--ticker SYMBOL", "Underlying ticker metadata for the import") { |value| options[:ticker] = value }
        opts.on("--option-root ROOT", "Filter source rows to a single option root") { |value| options[:option_root] = value }
        opts.on("--path PATH", "Massive flatfile CSV path to import") { |value| options[:path] = value }
        opts.on("--force", "Replace existing imported snapshot files") { options[:force] = true }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      options
    end

    def validate_import_options!(options)
      raise Tickrake::Error, "--type is required for imports." unless options[:type]
      raise Tickrake::Error, "Only --type options imports are supported." unless options[:type] == "options"
      raise Tickrake::Error, "Option imports require --provider." unless options[:provider]
      raise Tickrake::Error, "Option imports require --option-root." unless options[:option_root]
      raise Tickrake::Error, "Option imports require --path." unless options[:path]
    end

    def validate_import_job_options!(options)
      direct_args = [options[:type], options[:provider], options[:ticker], options[:option_root], options[:path]]
      raise Tickrake::Error, "Direct import arguments cannot be combined with --job." if direct_args.any?
    end

    def validate_job_run_options!(job, options)
      direct_args = [options[:ticker], options[:expiration_date], options[:option_root], options[:start_date], options[:end_date], options[:frequency]]
      raise Tickrake::Error, "--type cannot be combined with --job." if options[:type]
      raise Tickrake::Error, "Direct run arguments cannot be combined with --job." if direct_args.any?
      raise Tickrake::Error, "--scheduler requires --job." if options[:scheduler] && options[:job].nil?
      raise Tickrake::Error, "--supervisor requires --job." if options[:supervisor] && options[:job].nil?
      raise Tickrake::Error, "--scheduler cannot be combined with --supervisor." if options[:scheduler] && options[:supervisor]
      if job.manual? && (options[:scheduler] || options[:supervisor])
        raise Tickrake::Error, "Manual job `#{job.name}` cannot run as a scheduler. Use `tickrake run --job #{job.name}`."
      end
      if options[:from_config_start] && job.type != "candles"
        raise Tickrake::Error, "--from-config-start is only valid for candles jobs."
      end
    end

    def validate_direct_run_options!(options)
      raise Tickrake::Error, "--job is required for scheduler runs." if options[:scheduler]
      raise Tickrake::Error, "--type is required for direct runs." unless options[:type]

      case options[:type]
      when "options"
        raise Tickrake::Error, "Direct option runs require --ticker." unless options[:ticker]
        raise Tickrake::Error, "Direct option runs require --expiration-date." unless options[:expiration_date]
        raise Tickrake::Error, "--from-config-start is only valid for candles jobs." if options[:from_config_start]
      when "candles"
        raise Tickrake::Error, "Direct candle runs require --ticker." unless options[:ticker]
        raise Tickrake::Error, "Direct candle runs require --start-date." unless options[:start_date]
        raise Tickrake::Error, "Direct candle runs require --end-date." unless options[:end_date]
        raise Tickrake::Error, "Direct candle runs require --frequency." unless options[:frequency]
        raise Tickrake::Error, "--from-config-start cannot be combined with direct candle run arguments." if options[:from_config_start]
        raise Tickrake::Error, "--end-date must be on or after --start-date." if options[:end_date] < options[:start_date]
      else
        raise Tickrake::Error, "Unknown run type `#{options[:type]}`."
      end
    end

    def direct_options_universe(options)
      [Tickrake::OptionSymbol.new(symbol: options[:ticker], option_root: options[:option_root])]
    end

    def direct_candles_universe(options)
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
        expiration_date: options[:expiration_date],
        limit: options[:limit],
        ascending: options[:ascending],
        format: options[:format]
      )
      0
    end

    def storage_stats_command(argv, config)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      @stdout.puts(Tickrake::Storage::StatsReport.new(config).render)
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
        expiration_date: nil,
        limit: nil,
        ascending: true,
        format: "text"
      }
      parser = OptionParser.new do |opts|
        opts.on("--type TYPE", "Dataset type: candles or options") { |value| options[:type] = value }
        opts.on("--provider NAME", "Use the named provider namespace from config") { |value| options[:provider] = value }
        opts.on("--ticker SYMBOL", "Filter by ticker symbol") { |value| options[:ticker] = value }
        opts.on("--frequency FREQ", "Filter candle results by frequency") { |value| options[:frequency] = value }
        opts.on("--start-date YYYY-MM-DD", "Filter by dataset coverage start date") { |value| options[:start_date] = Date.iso8601(value) }
        opts.on("--end-date YYYY-MM-DD", "Filter by dataset coverage end date") { |value| options[:end_date] = Date.iso8601(value) }
        opts.on("--exp-date YYYY-MM-DD", "--expiration-date YYYY-MM-DD", "Filter option snapshots by expiration date") do |value|
          options[:expiration_date] = Date.iso8601(value)
        end
        opts.on("--limit N", Integer, "Limit matching option snapshots to N results") { |value| options[:limit] = value }
        opts.on("--ascending true|false", "Sort option snapshots by sample datetime ascending or descending") do |value|
          options[:ascending] = parse_boolean_option!(value, option_name: "--ascending")
        end
        opts.on("--format FORMAT", "Output format: text or json") { |value| options[:format] = value }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      options
    end

    def status_command(argv, config)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      registry = Tickrake::JobRegistry.new
      known_names = (config.jobs.map(&:name) + registry.registered_names).uniq.sort
      registry.statuses(known_names).each do |job|
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

    def logs_command(argv)
      options = parse_logs_options!(argv)
      log_path = Tickrake::PathSupport.named_log_path(options[:target])
      unless File.exist?(log_path)
        @stdout.puts("No log file at #{log_path}")
        return 0
      end

      content = File.read(log_path)
      @stdout.print(options[:tail] ? content.lines.last(options[:tail]).join : content)
      0
    end

    def parse_logs_options!(argv)
      options = { tail: nil, target: "cli" }
      options[:target] = argv.shift if argv.first && !argv.first.start_with?("-")
      parser = OptionParser.new do |opts|
        opts.on("--tail N", Integer, "Show only the last N log lines") { |value| options[:tail] = value }
      end
      parser.order!(argv)
      options[:target] = argv.shift if argv.first && !argv.first.start_with?("-")
      raise OptionParser::InvalidOption, argv.first if argv.any?

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

    def parse_boolean_option!(value, option_name:)
      normalized = value.to_s.strip.downcase
      return true if normalized == "true"
      return false if normalized == "false"

      raise Tickrake::Error, "#{option_name} must be true or false."
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

      if File.exist?(config_path) && !options[:force]
        raise Tickrake::Error, "Config already exists at #{config_path}. Use --force to overwrite it."
      end

      template_path = File.expand_path("../../config/tickrake.example.yml", __dir__)
      File.write(config_path, File.read(template_path))
      @stdout.puts("Initialized Tickrake home at #{home_dir}")
      @stdout.puts("Config written to #{config_path}")
      @stdout.puts("SQLite DB will be created at #{sqlite_path} on first run")
      @stdout.puts("CLI log file will be written to #{log_path}")
      0
    end

    def usage
      <<~TEXT
        Usage:
          tickrake init [--config path/to/tickrake.yml] [--force]
          tickrake validate-config [--config path/to/tickrake.yml] [--verbose]
          tickrake import --job JOB_NAME [--force] [--config path/to/tickrake.yml] [--verbose]
          tickrake import --type options --provider massive --option-root ROOT --path path/to/YYYY-MM-DD.csv [--ticker SYMBOL] [--force] [--config path/to/tickrake.yml] [--verbose]
          tickrake run --job JOB_NAME [--provider NAME] [--from-config-start] [--config path/to/tickrake.yml] [--verbose]
          tickrake run --type options --ticker SYMBOL --expiration-date YYYY-MM-DD [--option-root ROOT] [--provider NAME] [--config path/to/tickrake.yml] [--verbose]
          tickrake run --type candles --ticker SYMBOL --start-date YYYY-MM-DD --end-date YYYY-MM-DD --frequency FREQ [--provider NAME] [--config path/to/tickrake.yml] [--verbose]
          tickrake start --job JOB_NAME|all [--provider NAME] [--from-config-start] [--config path/to/tickrake.yml]
          tickrake stop --job JOB_NAME|all [--config path/to/tickrake.yml]
          tickrake restart --job JOB_NAME|all [--provider NAME] [--from-config-start] [--config path/to/tickrake.yml]
          tickrake status [--config path/to/tickrake.yml]
          tickrake query [--type candles|options] [--provider NAME] [--ticker SYMBOL] [--frequency FREQ] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--exp-date YYYY-MM-DD] [--limit N] [--ascending true|false] [--format text|json] [--config path/to/tickrake.yml]
          tickrake storage-stats [--config path/to/tickrake.yml]
          tickrake logs [TARGET] [--tail N]
      TEXT
    end
  end
end
