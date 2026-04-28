# frozen_string_literal: true

module Tickrake
  class ConfigLoader
    VALID_ADAPTERS = %w[schwab ibkr massive].freeze
    VALID_DAYS = %w[mon tue wed thu fri sat sun].freeze
    VALID_JOB_TYPES = %w[options candles].freeze
    VALID_IMPORT_TYPES = %w[options].freeze

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
      option_root_tickers = load_option_root_tickers(data.fetch("options", {}))
      data_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "data_dir", "~/.tickrake/data"))
      history_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "history_dir", File.join(data_dir, "history")))
      options_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "options_dir", File.join(data_dir, "options")))
      runtime = data.fetch("runtime", {})
      jobs = load_jobs(data.fetch("schedule", {}))
      import_jobs = load_import_jobs(data.fetch("imports", {}))

      config = Config.new(
        timezone: timezone,
        sqlite_path: sqlite_path,
        providers: providers,
        default_provider_name: default_provider_name,
        option_root_tickers: option_root_tickers,
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
        universe: Array(raw_job.fetch("universe")).map { |row| load_option_symbol(row) },
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
        universe: Array(raw_job.fetch("universe")).map { |row| load_candle_symbol(row) },
        manual: manual
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

    def validate_no_schedule_fields!(name, raw_job, fields)
      present = fields.select { |field| raw_job.key?(field) }
      return if present.empty?

      raise ConfigError, "manual job `#{name}` cannot define schedule fields: #{present.join(", ")}."
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

    def load_option_symbol(row)
      OptionSymbol.new(symbol: row.fetch("symbol"), option_root: row["option_root"], provider: row["provider"])
    end

    def load_candle_symbol(row)
      CandleSymbol.new(
        symbol: row.fetch("symbol"),
        provider: row["provider"],
        frequencies: Array(row.fetch("frequencies")).map { |value| normalize_frequency(value) }.uniq,
        start_date: Date.iso8601(row.fetch("start_date")),
        need_extended_hours_data: !!row.fetch("need_extended_hours_data", false),
        need_previous_close: !!row.fetch("need_previous_close", false)
      )
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
