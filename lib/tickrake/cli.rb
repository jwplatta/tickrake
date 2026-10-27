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
      e.respond_to?(:exit_status) ? e.exit_status : 1
    rescue OptionParser::ParseError => e
      @stderr.puts(e.message)
      1
    end

    private

    def dispatch(command, argv, common_options)
      config_path = common_options[:config_path]
      env_file = common_options[:env_file]

      case command
      when "validate-config"
        validate_config_command(argv, config_path, env_file)
      when "migrate"
        config = Tickrake::ConfigLoader.load(config_path, env_file: env_file)
        migrate_command(argv, config)
      when "publish-staging"
        config = Tickrake::ConfigLoader.load(config_path, env_file: env_file)
        publish_staging_command(argv, config, common_options)
      when "purge-staging"
        config = Tickrake::ConfigLoader.load(config_path, env_file: env_file)
        purge_staging_command(argv, config, common_options)
      else
        @stderr.puts(usage)
        1
      end
    end

    def validate_config_command(argv, config_path, env_file = nil)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      Tickrake::ConfigLoader.load(config_path, env_file: env_file)
      @stdout.puts("Config valid: #{config_path}")
      0
    end

    def migrate_command(argv, config)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      target = if config.respond_to?(:database) && config.database
                 config.database
               else
                 config.respond_to?(:sqlite_path) ? config.sqlite_path : Tickrake::PathSupport.sqlite_path
               end
      Tickrake::Tracker.migrate!(target)
      dest = if config.respond_to?(:database) && config.database&.postgres?
               "PostgreSQL database #{config.database.database || config.database.url}"
             elsif config.respond_to?(:database) && config.database&.sqlite_path
               "Tickrake database at #{config.database.sqlite_path}"
             else
               "Tickrake database at #{config.sqlite_path}"
             end
      @stdout.puts("Migrated #{dest}.")
      0
    end

    def publish_staging_command(argv, config, common_options)
      options = { datasets: nil, batch_size: nil, delete_after_upload: true, after: nil, before: nil }
      parser = OptionParser.new do |opts|
        opts.on("--dataset TYPE", "Dataset to publish (options, candles, level_one, order_book, fundamentals, economic_events, or all)") do |v|
          options[:datasets] = v.split(",").map(&:strip)
        end
        opts.on("--after DATE", "Publish files on or after this date (inclusive, YYYY-MM-DD)") do |v|
          options[:after] = parse_cli_date_option!(v, option_name: "--after")
        end
        opts.on("--before DATE", "Publish files before this date (exclusive, YYYY-MM-DD)") do |v|
          options[:before] = parse_cli_date_option!(v, option_name: "--before")
        end
        opts.on("--batch-size N", Integer, "Maximum number of files to upload") { |v| options[:batch_size] = v }
        opts.on("--no-delete", "Do not delete published local files after upload") { options[:delete_after_upload] = false }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      if options[:after] && options[:before] && options[:after] >= options[:before]
        raise Tickrake::Error, "--after must be before --before."
      end

      runtime = Tickrake::Runtime.new(
        config: config,
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path("staging_publisher"),
        config_path: common_options[:config_path],
        context: { job_name: "staging_publisher", job_type: "staging_publish" }
      )

      publisher = Tickrake::Storage::StagingPublisher.new(runtime: runtime)
      summary = publisher.scan_and_publish(
        dataset_types: options[:datasets],
        batch_size: options[:batch_size],
        delete_after_upload: options[:delete_after_upload],
        after: options[:after],
        before: options[:before]
      )
      @stdout.puts("Staging publish complete: scanned #{summary[:files_scanned]} file(s), uploaded #{summary[:files_uploaded]} file(s), deleted #{summary[:files_deleted]} file(s).")
      0
    end

    def purge_staging_command(argv, config, common_options)
      options = {
        datasets: nil,
        date: nil,
        provider: nil,
        dry_run: false,
        remote: false,
        local: false
      }
      parser = OptionParser.new do |opts|
        opts.on("-d", "--date DATE", "Date to purge (required, YYYY-MM-DD)") do |v|
          options[:date] = parse_cli_date_option!(v, option_name: "--date")
        end
        opts.on("--dataset TYPE", "Dataset to purge (options, candles, level_one, order_book, fundamentals, economic_events, or all)") do |v|
          options[:datasets] = v.split(",").map(&:strip)
        end
        opts.on("-p", "--provider NAME", "Provider name filter (e.g. schwab)") { |v| options[:provider] = v }
        opts.on("--remote", "Purge from remote staging datastore") { options[:remote] = true }
        opts.on("--local", "Purge from local data directory") { options[:local] = true }
        opts.on("--all", "Purge from both remote datastore and local directory") do
          options[:remote] = true
          options[:local] = true
        end
        opts.on("--dry-run", "Preview matching files and keys without deleting") { options[:dry_run] = true }
      end
      parser.order!(argv)
      raise OptionParser::InvalidOption, argv.first if argv.any?

      raise Tickrake::Error, "--date YYYY-MM-DD is required for purge-staging." unless options[:date]

      # Default to remote purge if neither --remote nor --local was explicitly specified
      unless options[:remote] || options[:local]
        options[:remote] = true
      end

      runtime = Tickrake::Runtime.new(
        config: config,
        verbose: common_options[:verbose],
        stdout: @stdout,
        log_path: Tickrake::PathSupport.named_log_path("staging_purger"),
        config_path: common_options[:config_path],
        context: { job_name: "staging_purger", job_type: "staging_purge" }
      )

      purger = Tickrake::Storage::StagingPurger.new(runtime: runtime)

      summary = purger.purge(
        dataset_types: options[:datasets],
        date: options[:date],
        provider: options[:provider],
        dry_run: options[:dry_run],
        purge_remote: options[:remote],
        purge_local: options[:local]
      )

      prefix = options[:dry_run] ? "[DRY RUN] " : ""
      parts = []
      if options[:remote]
        parts << "remote keys: scanned #{summary[:remote_keys_scanned]}, deleted #{summary[:remote_keys_deleted]}"
      end
      if options[:local]
        parts << "local files: scanned #{summary[:local_files_scanned]}, deleted #{summary[:local_files_deleted]}"
      end

      @stdout.puts("#{prefix}Staging purge complete: #{parts.join('; ')}.")
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
      options = { config_path: Tickrake::PathSupport.config_path, env_file: nil, verbose: false }

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
        when "--env-file"
          value = argv[index + 1]
          raise OptionParser::MissingArgument, "--env-file" if value.nil?

          options[:env_file] = value
          argv.slice!(index, 2)
        when /\A--env-file=(.+)\z/
          options[:env_file] = Regexp.last_match(1)
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

    def parse_cli_date_option!(value, option_name:)
      str = value.to_s.strip
      if str.include?("T") || str.include?(":")
        Time.iso8601(str)
      else
        Date.iso8601(str)
      end
    rescue ArgumentError
      raise Tickrake::Error, "#{option_name} must be a valid ISO8601 date (YYYY-MM-DD) or timestamp."
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
      @stdout.puts("SQLite DB will be created at #{sqlite_path} when you run `tickrake migrate`")
      @stdout.puts("CLI log file will be written to #{log_path}")
      0
    end

    def usage
      <<~TEXT
        Usage:
          tickrake init [--config path/to/tickrake.yml] [--force]
          tickrake validate-config [--config path/to/tickrake.yml] [--env-file path/to/.env] [--verbose]
          tickrake migrate [--config path/to/tickrake.yml] [--env-file path/to/.env]
          tickrake publish-staging [--dataset TYPE] [--after DATE] [--before DATE] [--batch-size N] [--no-delete] [--config path/to/tickrake.yml] [--env-file path/to/.env] [--verbose]
          tickrake purge-staging --date YYYY-MM-DD [--dataset TYPE] [--provider NAME] [--remote] [--local] [--all] [--dry-run] [--config path/to/tickrake.yml] [--env-file path/to/.env] [--verbose]
          tickrake logs [TARGET] [--tail N]
      TEXT
    end
  end
end
