# frozen_string_literal: true

require "tzinfo"

module Tickrake
  class ConfigLoader
    VALID_ADAPTERS = %w[schwab ibkr massive].freeze
    VALID_DAYS = %w[mon tue wed thu fri sat sun].freeze
    VALID_JOB_TYPES = %w[options candles maintenance].freeze
    VALID_IMPORT_TYPES = %w[options].freeze
    VALID_MAINTENANCE_ACTIONS = %w[compact archive].freeze
    VALID_MAINTENANCE_SUBJECTS = %w[option_samples].freeze
    VALID_ARCHIVE_DESTINATIONS = %w[s3_archive].freeze
    VALID_ARCHIVE_ARTIFACTS = %w[csv parquet].freeze
    VALID_S3_STORAGE_CLASSES = %w[STANDARD GLACIER GLACIER_IR].freeze

    def self.load(path)
      new(path).load
    end

    def initialize(path)
      @path = File.expand_path(path)
    end

    def load
      data = YAML.safe_load(File.read(@path), permitted_classes: [Date], aliases: true) || {}

      timezone = data.fetch("timezone", ENV.fetch("TZ", "America/Chicago"))
      sqlite_path = Tickrake::PathSupport.expand_path(data.fetch("sqlite_path", Tickrake::PathSupport.sqlite_path))
      providers, default_provider_name = load_providers(data)
      options_config = data.fetch("options", {})
      option_root_tickers = load_option_root_tickers(options_config)
      option_snapshot_filename_timezone = load_option_snapshot_filename_timezone(options_config)
      data_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "data_dir", "~/.tickrake/data"))
      history_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "history_dir", File.join(data_dir, "history")))
      options_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "options_dir", File.join(data_dir, "options")))
      archives = load_archives(data.fetch("storage", {}))
      universes = load_universes(data.fetch("universes", {}))
      @universes = universes
      runtime = data.fetch("runtime", {})
      jobs = load_jobs(data.fetch("schedule", {}))
      import_jobs = load_import_jobs(data.fetch("imports", {}))

      config = Config.new(
        timezone: timezone,
        sqlite_path: sqlite_path,
        providers: providers,
        default_provider_name: default_provider_name,
        option_root_tickers: option_root_tickers,
        option_snapshot_filename_timezone: option_snapshot_filename_timezone,
        archives: archives,
        universes: universes,
        data_dir: data_dir,
        history_dir: history_dir,
        options_dir: options_dir,
        max_workers: Integer(runtime.fetch("max_workers", 4)),
        retry_count: Integer(runtime.fetch("retry_count", 2)),
        retry_delay_seconds: Integer(runtime.fetch("retry_delay_seconds", 2)),
        option_fetch_timeout_seconds: Integer(runtime.fetch("option_fetch_timeout_seconds", 30)),
        candle_fetch_timeout_seconds: Integer(runtime.fetch("candle_fetch_timeout_seconds", 60)),
        jobs: jobs,
        import_jobs: import_jobs
      )

      validate!(config)
      config
    end

    private

    def validate!(config)
      raise ConfigError, "At least one provider is required." if config.providers.empty?
      config.providers.each_value do |provider|
        raise ConfigError, "Unsupported provider adapter: #{provider.adapter}" unless VALID_ADAPTERS.include?(provider.adapter)
      end

      config.provider_definition(config.default_provider_name)
      raise ConfigError, "At least one scheduled or import job is required." if config.jobs.empty? && config.import_jobs.empty?

      config.jobs.each do |job|
        raise ConfigError, "Unknown job type `#{job.type}` for `#{job.name}`." unless VALID_JOB_TYPES.include?(job.type)
        config.provider_definition(job.provider) if job.provider

        if job.options?
          unless job.manual?
            raise ConfigError, "options job `#{job.name}` interval must be positive." if job.interval_seconds.to_i <= 0
            raise ConfigError, "At least one options job window is required for `#{job.name}`." if job.windows.empty?
          end
          raise ConfigError, "At least one options universe symbol is required for `#{job.name}`." if job.universe.empty?
          job.universe.each { |entry| config.provider_definition(entry.provider) if entry.provider }
        elsif job.candles?
          raise ConfigError, "At least one candle universe symbol is required for `#{job.name}`." if job.universe.empty?
          raise ConfigError, "candle lookback_days must be non-negative for `#{job.name}`." if job.lookback_days.to_i.negative?
          validate_candle_schedule!(job)
          job.universe.each { |entry| config.provider_definition(entry.provider) if entry.provider }
        elsif job.maintenance?
          validate_maintenance_schedule!(job)
          validate_maintenance_tasks!(config, job)
        end
      end

      config.import_jobs.each do |job|
        raise ConfigError, "Unknown import type `#{job.type}` for `#{job.name}`." unless VALID_IMPORT_TYPES.include?(job.type)
        config.provider_definition(job.provider)
        raise ConfigError, "import job `#{job.name}` requires option_root." if job.option_root.to_s.empty?
        raise ConfigError, "import job `#{job.name}` requires at least one path." if job.paths.empty?
      end

      raise ConfigError, "max_workers must be positive." if config.max_workers <= 0
    end

    def load_option_snapshot_filename_timezone(raw_options)
      return "utc" if raw_options.nil?
      raise ConfigError, "options must be a mapping." unless raw_options.is_a?(Hash)

      raw_value = raw_options.fetch("snapshot_filename_timezone", "utc").to_s.strip
      return "utc" if raw_value.casecmp("utc").zero?

      TZInfo::Timezone.get(raw_value)
      raw_value
    rescue TZInfo::InvalidTimezoneIdentifier
      raise ConfigError, "Invalid options.snapshot_filename_timezone: #{raw_value}"
    end

    def load_archives(raw_storage)
      raise ConfigError, "storage must be a mapping." unless raw_storage.is_a?(Hash)

      raw_s3_archive = raw_storage["s3_archive"]
      return {} if raw_s3_archive.nil?

      raise ConfigError, "storage.s3_archive must be a mapping." unless raw_s3_archive.is_a?(Hash)

      bucket = raw_s3_archive.fetch("bucket", "").to_s.strip
      raise ConfigError, "storage.s3_archive.bucket is required." if bucket.empty?

      region = raw_s3_archive.fetch("region", nil)&.to_s&.strip
      prefix = raw_s3_archive.fetch("prefix", "").to_s
      storage_class = raw_s3_archive.fetch("storage_class", "GLACIER_IR").to_s.strip.upcase
      unless VALID_S3_STORAGE_CLASSES.include?(storage_class)
        raise ConfigError, "Invalid storage.s3_archive.storage_class: #{storage_class}"
      end

      {
        "s3_archive" => S3ArchiveConfig.new(
          bucket: bucket,
          region: region.nil? || region.empty? ? nil : region,
          prefix: prefix,
          storage_class: storage_class
        )
      }
    end

    def load_universes(raw_universes)
      raise ConfigError, "universes must be a mapping." unless raw_universes.is_a?(Hash)

      merged_universes = raw_universes.dup
      if merged_universes.key?("file")
        file_universes = load_universes_file(merged_universes.delete("file"))
        merged_universes = file_universes.merge(merged_universes)
      end

      merged_universes.each_with_object({}) do |(name, raw_universe), universes|
        universes[name.to_s] = load_universe(name, raw_universe)
      end
    end

    def load_universes_file(relative_path)
      path = File.expand_path(relative_path.to_s, File.dirname(@path))
      raise ConfigError, "universes file not found: #{path}" unless File.exist?(path)

      data = YAML.safe_load(File.read(path), permitted_classes: [Date], aliases: true) || {}
      raise ConfigError, "universes file must contain a mapping." unless data.is_a?(Hash)

      universes = data.key?("universes") ? data.fetch("universes") : data
      raise ConfigError, "universes file must define a universes mapping." unless universes.is_a?(Hash)

      universes
    end

    def load_universe(name, raw_universe)
      raise ConfigError, "universe `#{name}` must be a mapping or array." unless raw_universe.is_a?(Hash) || raw_universe.is_a?(Array)

      data =
        if raw_universe.is_a?(Hash) && raw_universe.key?("file")
          load_universe_file(name, raw_universe.fetch("file"))
        else
          raw_universe
        end

      entries = load_universe_entries(name, data)
      raise ConfigError, "universe `#{name}` requires symbols." if entries.empty?

      UniverseConfig.new(name: name.to_s, entries: entries)
    end

    def load_universe_file(name, relative_path)
      path = File.expand_path(relative_path.to_s, File.dirname(@path))
      raise ConfigError, "universe `#{name}` file not found: #{path}" unless File.exist?(path)

      data = YAML.safe_load(File.read(path), permitted_classes: [Date], aliases: true) || {}
      raise ConfigError, "universe `#{name}` file must contain a mapping or array." unless data.is_a?(Hash) || data.is_a?(Array)

      data
    end

    def load_universe_entries(name, data)
      raw_entries =
        case data
        when Array
          data
        when Hash
          if data.key?("symbols")
            Array(data.fetch("symbols"))
          elsif data.key?("entries")
            Array(data.fetch("entries"))
          elsif data.key?("option_roots")
            Array(data.fetch("option_roots")).map { |value| { "symbol" => value.to_s } }
          else
            []
          end
        else
          []
        end

      raw_entries.map { |entry| load_universe_entry(name, entry) }
    end

    def load_universe_entry(name, raw_entry)
      case raw_entry
      when String
        UniverseEntry.new(
          symbol: raw_entry,
          option_root: nil,
          option_roots: [],
          start_date: nil,
          need_extended_hours_data: false,
          need_previous_close: false
        )
      when Hash
        row = stringify_keys(raw_entry)
        symbol = row.fetch("symbol").to_s
        raise ConfigError, "universe `#{name}` entries require symbol." if symbol.empty?
        raise ConfigError, "universe `#{name}` entries cannot define frequencies; set frequencies on the candles job." if row.key?("frequencies")
        raise ConfigError, "universe `#{name}` entries cannot define provider; set provider on the job." if row.key?("provider")

        UniverseEntry.new(
          symbol: symbol,
          option_root: row["option_root"]&.to_s,
          option_roots: Array(row["option_roots"]).map(&:to_s),
          start_date: row["start_date"] ? Date.iso8601(row.fetch("start_date").to_s) : nil,
          need_extended_hours_data: !!row.fetch("need_extended_hours_data", false),
          need_previous_close: !!row.fetch("need_previous_close", false)
        )
      else
        raise ConfigError, "universe `#{name}` entries must be strings or mappings."
      end
    end

    def load_jobs(schedule)
      raise ConfigError, "schedule must be a mapping." unless schedule.is_a?(Hash)

      schedule.map do |name, raw_job|
        build_job(name, raw_job)
      end
    end

    def load_import_jobs(imports)
      raise ConfigError, "imports must be a mapping." unless imports.is_a?(Hash)

      imports.map do |name, raw_job|
        build_import_job(name, raw_job)
      end
    end

    def build_import_job(name, raw_job)
      raise ConfigError, "import job `#{name}` must be a mapping." unless raw_job.is_a?(Hash)
      raise ConfigError, "import job `#{name}` must define type." unless raw_job.key?("type")

      type = raw_job.fetch("type").to_s
      raise ConfigError, "Unknown import type `#{type}` for `#{name}`." unless VALID_IMPORT_TYPES.include?(type)

      ImportJobConfig.new(
        name: name.to_s,
        type: type,
        provider: raw_job.fetch("provider", nil).to_s,
        ticker: raw_job["ticker"],
        option_root: raw_job["option_root"],
        paths: Array(raw_job.fetch("paths")).map(&:to_s),
        force: !!raw_job.fetch("force", false)
      )
    end

    def build_job(name, raw_job)
      raise ConfigError, "job `#{name}` must be a mapping." unless raw_job.is_a?(Hash)
      raise ConfigError, "job `#{name}` must define type." unless raw_job.key?("type")

      type = raw_job.fetch("type").to_s
      raise ConfigError, "Unknown job type `#{type}` for `#{name}`." unless VALID_JOB_TYPES.include?(type)

      case type
      when "options"
        build_options_job(name, raw_job)
      when "candles"
        build_candles_job(name, raw_job)
      when "maintenance"
        build_maintenance_job(name, raw_job)
      end
    end

    def build_options_job(name, raw_job)
      manual = manual_job?(raw_job)
      validate_no_schedule_fields!(name, raw_job, %w[interval_seconds windows]) if manual

      ScheduledJobConfig.new(
        name: name.to_s,
        type: "options",
        provider: raw_job["provider"],
        interval_seconds: manual ? nil : Integer(raw_job.fetch("interval_seconds")),
        windows: manual ? [] : load_scheduler_windows(raw_job.fetch("windows")),
        run_at: nil,
        days: [],
        lookback_days: nil,
        dte_buckets: Array(raw_job.fetch("dte_buckets")).map { |bucket| parse_bucket(bucket) },
        universe: load_options_universe(raw_job),
        tasks: [],
        task: nil,
        settings: {},
        manual: manual
      )
    end

    def build_candles_job(name, raw_job)
      manual = manual_job?(raw_job)
      validate_no_schedule_fields!(name, raw_job, %w[interval_seconds windows run_at days]) if manual
      uses_interval_schedule = raw_job.key?("interval_seconds") || raw_job.key?("windows")
      uses_daily_schedule = raw_job.key?("run_at") || raw_job.key?("days")

      ScheduledJobConfig.new(
        name: name.to_s,
        type: "candles",
        provider: raw_job["provider"],
        interval_seconds: uses_interval_schedule ? Integer(raw_job.fetch("interval_seconds")) : nil,
        windows: uses_interval_schedule ? load_scheduler_windows(raw_job.fetch("windows")) : [],
        run_at: uses_daily_schedule ? normalize_clock(raw_job.fetch("run_at")) : nil,
        days: uses_daily_schedule ? normalize_days(raw_job.fetch("days")) : [],
        lookback_days: Integer(raw_job.fetch("lookback_days")),
        dte_buckets: [],
        universe: load_candles_universe(raw_job),
        tasks: [],
        task: nil,
        settings: {},
        manual: manual
      )
    end

    def build_maintenance_job(name, raw_job)
      manual = manual_job?(raw_job)
      validate_no_schedule_fields!(name, raw_job, %w[interval_seconds windows run_at days]) if manual
      uses_interval_schedule = raw_job.key?("interval_seconds") || raw_job.key?("windows")
      uses_daily_schedule = raw_job.key?("run_at") || raw_job.key?("days")
      raise ConfigError, "maintenance job `#{name}` must use tasks: and no longer supports task:/settings:." if raw_job.key?("task") || raw_job.key?("settings")

      ScheduledJobConfig.new(
        name: name.to_s,
        type: "maintenance",
        provider: raw_job["provider"],
        interval_seconds: uses_interval_schedule ? Integer(raw_job.fetch("interval_seconds")) : nil,
        windows: uses_interval_schedule ? load_scheduler_windows(raw_job.fetch("windows")) : [],
        run_at: uses_daily_schedule ? normalize_clock(raw_job.fetch("run_at")) : nil,
        days: uses_daily_schedule ? normalize_days(raw_job.fetch("days")) : [],
        lookback_days: nil,
        dte_buckets: [],
        universe: [],
        tasks: Array(raw_job.fetch("tasks")).map { |row| load_maintenance_step(name, row) },
        task: nil,
        settings: {},
        manual: manual
      )
    end

    def load_maintenance_step(job_name, raw_step)
      raise ConfigError, "maintenance job `#{job_name}` task entry must be a mapping." unless raw_step.is_a?(Hash)
      raise ConfigError, "maintenance job `#{job_name}` task entry must define exactly one action." unless raw_step.keys.length == 1

      action = raw_step.keys.first.to_s
      raise ConfigError, "Unknown maintenance action `#{action}` for `#{job_name}`." unless VALID_MAINTENANCE_ACTIONS.include?(action)

      settings = stringify_keys(raw_step.fetch(action))
      raise ConfigError, "maintenance job `#{job_name}` action `#{action}` settings must be a mapping." unless settings.is_a?(Hash)

      MaintenanceStepConfig.new(
        action: action,
        subject: settings.fetch("subject", "").to_s,
        provider: settings["provider"]&.to_s,
        universe: settings["universe"]&.to_s,
        universes: Array(settings["universes"]).map(&:to_s),
        tickers: Array(settings["tickers"]),
        option_root: settings["option_root"]&.to_s,
        delete_sources: settings.fetch("delete_sources", false),
        destination: settings["destination"]&.to_s,
        artifacts: Array(settings["artifacts"]).map(&:to_s),
        retain_local: stringify_keys(settings["retain_local"])
      )
    end

    def validate_candle_schedule!(job)
      return if job.manual?

      if job.interval_schedule? && job.daily_schedule?
        raise ConfigError, "candles job `#{job.name}` must use either interval_seconds/windows or run_at/days, not both."
      end
      if !job.interval_schedule? && !job.daily_schedule?
        raise ConfigError, "candles job `#{job.name}` must define either interval_seconds/windows or run_at/days."
      end
      if job.interval_schedule?
        raise ConfigError, "candles job `#{job.name}` interval must be positive." if job.interval_seconds.to_i <= 0
        raise ConfigError, "At least one candles job window is required for `#{job.name}`." if job.windows.empty?
      end
      if job.daily_schedule?
        raise ConfigError, "At least one candles job day is required for `#{job.name}`." if job.days.empty?
      end
    end

    def manual_job?(raw_job)
      raw_job.fetch("manual", false) == true
    end

    def validate_maintenance_schedule!(job)
      return if job.manual?

      if job.interval_schedule? && job.daily_schedule?
        raise ConfigError, "maintenance job `#{job.name}` must use either interval_seconds/windows or run_at/days, not both."
      end
      if !job.interval_schedule? && !job.daily_schedule?
        raise ConfigError, "maintenance job `#{job.name}` must define either interval_seconds/windows or run_at/days."
      end
      if job.interval_schedule?
        raise ConfigError, "maintenance job `#{job.name}` interval must be positive." if job.interval_seconds.to_i <= 0
        raise ConfigError, "At least one maintenance job window is required for `#{job.name}`." if job.windows.empty?
      end
      if job.daily_schedule?
        raise ConfigError, "At least one maintenance job day is required for `#{job.name}`." if job.days.empty?
      end
    end

    def validate_maintenance_tasks!(config, job)
      raise ConfigError, "maintenance job `#{job.name}` requires at least one task." if Array(job.tasks).empty?

      Array(job.tasks).each do |step|
        raise ConfigError, "maintenance job `#{job.name}` requires a valid subject." unless VALID_MAINTENANCE_SUBJECTS.include?(step.subject)
        config.provider_definition(step.provider) if step.provider
        validate_maintenance_target!(config, job, step)

        case step.action
        when "compact"
          raise ConfigError, "maintenance job `#{job.name}` compact task delete_sources must be boolean." unless boolean?(step.delete_sources)
        when "archive"
          raise ConfigError, "maintenance job `#{job.name}` archive task destination is required." if step.destination.to_s.empty?
          unless VALID_ARCHIVE_DESTINATIONS.include?(step.destination)
            raise ConfigError, "maintenance job `#{job.name}` archive task destination `#{step.destination}` is unsupported."
          end
          raise ConfigError, "maintenance job `#{job.name}` archive task destination `#{step.destination}` is not configured." if config.archives[step.destination].nil?

          artifacts = step.artifacts.empty? ? VALID_ARCHIVE_ARTIFACTS : step.artifacts
          unless artifacts.all? { |artifact| VALID_ARCHIVE_ARTIFACTS.include?(artifact) }
            raise ConfigError, "maintenance job `#{job.name}` archive task artifacts must be csv and/or parquet."
          end
          unless step.retain_local.keys.all? { |key| VALID_ARCHIVE_ARTIFACTS.include?(key) && boolean?(step.retain_local[key]) }
            raise ConfigError, "maintenance job `#{job.name}` archive task retain_local keys must be csv/parquet booleans."
          end
        end
      end
    end

    def validate_maintenance_target!(config, job, step)
      has_universe = !step.universe.to_s.empty?
      has_universes = !Array(step.universes).empty?
      has_tickers = !Array(step.tickers).empty?
      has_option_root = !step.option_root.to_s.empty?
      raise ConfigError, "maintenance job `#{job.name}` task `#{step.action}` requires universe, universes, tickers, or option_root." unless has_universe || has_universes || has_tickers || has_option_root
      raise ConfigError, "maintenance job `#{job.name}` task `#{step.action}` cannot combine option_root with universe/universes/tickers." if has_option_root && (has_universe || has_universes || has_tickers)

      config.universe(step.universe) if has_universe
      Array(step.universes).each { |name| config.universe(name) }
    end

    def validate_no_schedule_fields!(name, raw_job, fields)
      present = fields.select { |field| raw_job.key?(field) }
      return if present.empty?

      raise ConfigError, "manual job `#{name}` cannot define schedule fields: #{present.join(', ')}."
    end

    def load_scheduler_windows(raw_windows)
      Array(raw_windows).map do |window|
        SchedulerWindow.new(
          days: normalize_days(window.fetch("days")),
          start_time: normalize_clock(window.fetch("start")),
          end_time: normalize_clock(window.fetch("end"))
        )
      end
    end

    def load_options_universe(raw_job)
      load_job_entries(raw_job).flat_map do |entry|
        expand_option_universe_entry(entry, raw_job)
      end
    end

    def load_candles_universe(raw_job)
      expand_candle_entries(load_job_entries(raw_job), raw_job, universe_name: "job")
    end

    def expand_option_universe_entry(entry, raw_job)
      default_roots = Array(raw_job["option_roots"]).map(&:to_s)
      default_root = raw_job["option_root"]&.to_s
      roots = Array(entry.option_roots).map(&:to_s)
      roots << entry.option_root.to_s unless entry.option_root.to_s.empty?
      roots.concat(default_roots)
      roots << default_root unless default_root.to_s.empty?
      roots = roots.reject(&:empty?).uniq
      roots = [nil] if roots.empty?

      roots.map do |option_root|
        OptionSymbol.new(symbol: entry.symbol, option_root: option_root)
      end
    end

    def expand_candle_entries(entries, raw_job, universe_name:)
      default_frequencies = Array(raw_job["frequencies"]).map { |value| normalize_frequency(value) }.uniq
      default_start_date = raw_job["start_date"] ? Date.iso8601(raw_job.fetch("start_date").to_s) : nil
      default_extended_hours = !!raw_job.fetch("need_extended_hours_data", false)
      default_previous_close = !!raw_job.fetch("need_previous_close", false)

      entries.map do |entry|
        start_date = entry.start_date || default_start_date
        raise ConfigError, "candles job universe `#{universe_name}` requires job-level frequencies." if default_frequencies.empty?
        raise ConfigError, "candles job universe `#{universe_name}` requires start_date for #{entry.symbol}." if start_date.nil?

        CandleSymbol.new(
          symbol: entry.symbol,
          provider: nil,
          frequencies: default_frequencies,
          start_date: start_date,
          need_extended_hours_data: entry.need_extended_hours_data || default_extended_hours,
          need_previous_close: entry.need_previous_close || default_previous_close
        )
      end
    end

    def load_job_entries(raw_job)
      entries = []

      case raw_job["universe"]
      when String
        entries.concat(universe(raw_job["universe"]).entries)
      when Array
        entries.concat(Array(raw_job["universe"]).map { |row| load_inline_universe_entry("job", row) })
      when nil
      else
        raise ConfigError, "job universe must be a string or array."
      end

      Array(raw_job["universes"]).each do |name|
        entries.concat(universe(name).entries)
      end

      Array(raw_job["tickers"]).each do |row|
        entries << load_inline_universe_entry("job", row)
      end

      entries
    end

    def load_inline_universe_entry(job_type, row)
      load_universe_entry("#{job_type} job", row)
    end

    def universe(name)
      selected = @universes.fetch(name.to_s, nil)
      raise ConfigError, "Unknown universe `#{name}`." unless selected

      selected
    end

    def load_providers(data)
      raise ConfigError, "providers must be configured." unless data.key?("providers")
      raise ConfigError, "default_provider must be configured." unless data.key?("default_provider")

      providers = parse_named_providers(data.fetch("providers"))
      default_provider_name = data.fetch("default_provider")
      [providers, default_provider_name.to_s]
    end

    def load_option_root_tickers(raw_options)
      return {} if raw_options.nil?
      raise ConfigError, "options must be a mapping." unless raw_options.is_a?(Hash)

      raw_mapping = raw_options.fetch("root_tickers", {})
      raise ConfigError, "options.root_tickers must be a mapping." unless raw_mapping.is_a?(Hash)

      raw_mapping.each_with_object({}) do |(root, ticker), mapping|
        mapping[root.to_s.upcase] = ticker.to_s.upcase
      end
    end

    def parse_named_providers(raw_providers)
      raise ConfigError, "providers must be a mapping." unless raw_providers.is_a?(Hash)

      raw_providers.each_with_object({}) do |(name, raw_provider), providers|
        raise ConfigError, "provider `#{name}` must be a mapping." unless raw_provider.is_a?(Hash)

        adapter = raw_provider.fetch("adapter").to_s
        settings = stringify_keys(raw_provider.fetch("settings", {}))
        symbol_map = stringify_symbol_map(raw_provider.fetch("symbol_map", {}))
        provider_name = name.to_s
        providers[provider_name] = ProviderDefinition.new(
          name: provider_name,
          adapter: adapter,
          settings: settings,
          symbol_map: symbol_map
        )
      end
    end

    def stringify_keys(value)
      return {} if value.nil?
      raise ConfigError, "provider settings must be a mapping." unless value.is_a?(Hash)

      value.each_with_object({}) do |(key, child), hash|
        hash[key.to_s] = child.is_a?(Hash) ? stringify_keys(child) : child
      end
    end

    def stringify_symbol_map(value)
      return {} if value.nil?
      raise ConfigError, "provider symbol_map must be a mapping." unless value.is_a?(Hash)

      value.each_with_object({}) do |(key, child), hash|
        hash[key.to_s] = child.to_s
      end
    end

    def dig(hash, *keys)
      default = keys.pop
      keys.reduce(hash) { |value, key| value.is_a?(Hash) ? value[key] : nil } || default
    end

    def boolean?(value)
      value == true || value == false
    end

    def parse_bucket(bucket)
      match = /\A(\d+)DTE\z/.match(bucket.to_s.strip.upcase)
      raise ConfigError, "Invalid DTE bucket: #{bucket}" unless match

      Integer(match[1])
    end

    def normalize_days(days)
      Array(days).map do |day|
        normalized = day.to_s.downcase[0, 3]
        raise ConfigError, "Invalid weekday: #{day}" unless VALID_DAYS.include?(normalized)

        normalized
      end
    end

    def normalize_clock(value)
      match = /\A(\d{2}):(\d{2})\z/.match(value.to_s)
      raise ConfigError, "Invalid clock value: #{value}" unless match

      hour = Integer(match[1], 10)
      minute = Integer(match[2], 10)
      raise ConfigError, "Invalid clock value: #{value}" unless hour.between?(0, 23) && minute.between?(0, 59)

      [hour, minute]
    end

    def normalize_frequency(value)
      normalized = value.to_s.downcase.strip
      aliases = {
        "minute" => "1min",
        "1m" => "1min",
        "1min" => "1min",
        "5m" => "5min",
        "5min" => "5min",
        "10m" => "10min",
        "10min" => "10min",
        "15m" => "15min",
        "15min" => "15min",
        "30m" => "30min",
        "30min" => "30min",
        "day" => "day",
        "daily" => "day",
        "week" => "week",
        "weekly" => "week",
        "month" => "month",
        "monthly" => "month"
      }
      return aliases.fetch(normalized) if aliases.key?(normalized)

      raise ConfigError, "Unsupported candle frequency: #{value}"
    end
  end
end
