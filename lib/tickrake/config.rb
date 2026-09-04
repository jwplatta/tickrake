# frozen_string_literal: true

module Tickrake
  S3ArchiveConfig = Struct.new(:bucket, :region, :prefix, :storage_class, keyword_init: true) do
    def prefixed_key(relative_path)
      normalized_relative_path = relative_path.to_s.sub(%r{\A/+}, "")
      normalized_prefix = prefix.to_s.gsub(%r{\A/+|/+\z}, "")
      return normalized_relative_path if normalized_prefix.empty?

      [normalized_prefix, normalized_relative_path].join("/")
    end
  end

  UniverseEntry = Struct.new(
    :symbol,
    :option_root,
    :option_roots,
    :start_date,
    :need_extended_hours_data,
    :need_previous_close,
    keyword_init: true
  )
  UniverseConfig = Struct.new(:name, :entries, keyword_init: true) do
    def symbols
      entries.map(&:symbol)
    end

    def option_roots
      entries.flat_map do |entry|
        roots = if Array(entry.option_roots).any?
                  entry.option_roots
                elsif !entry.option_root.to_s.empty?
                  [entry.option_root]
                else
                  [entry.symbol]
                end

        roots.map { |root| root.to_s.strip.upcase }.reject(&:empty?)
      end.uniq
    end
  end
  MaintenanceStepConfig = Struct.new(
    :action,
    :subject,
    :provider,
    :universe,
    :universes,
    :tickers,
    :option_root,
    :delete_sources,
    :destination,
    :artifacts,
    :retain_local,
    keyword_init: true
  )

  SchedulerWindow = Struct.new(:days, :start_time, :end_time, keyword_init: true)
  OptionSymbol = Struct.new(:symbol, :option_root, :provider, keyword_init: true)
  ProviderDefinition = Struct.new(:name, :adapter, :settings, :symbol_map, keyword_init: true) do
    def rate_limit_max_requests
      configured = settings.fetch("rate_limit_max_requests", nil)
      Integer(configured) if configured
    end

    def rate_limit_interval_seconds
      configured = settings.fetch("rate_limit_interval_seconds", nil)
      Integer(configured) if configured
    end

    def restart_after_consecutive_failures
      configured = settings.fetch("restart_after_consecutive_failures", nil)
      return 3 if configured.nil? && adapter == "schwab"
      return nil if configured.nil?

      Integer(configured)
    end

    def restart_cooldown_seconds
      configured = settings.fetch("restart_cooldown_seconds", nil)
      return Integer(configured) unless configured.nil?
      return 30 if adapter == "schwab" && !restart_after_consecutive_failures.nil?

      nil
    end
  end
  CandleSymbol = Struct.new(
    :symbol,
    :provider,
    :frequencies,
    :start_date,
    :need_extended_hours_data,
    :need_previous_close,
    keyword_init: true
  )
  ScheduledJobConfig = Struct.new(
    :name,
    :type,
    :provider,
    :interval_seconds,
    :windows,
    :run_at,
    :days,
    :lookback_days,
    :dte_buckets,
    :universe,
    :tasks,
    :task,
    :settings,
    :manual,
    keyword_init: true
  ) do
    def options?
      type == "options"
    end

    def candles?
      type == "candles"
    end

    def maintenance?
      type == "maintenance"
    end

    def interval_schedule?
      !interval_seconds.nil?
    end

    def daily_schedule?
      !run_at.nil?
    end

    def manual?
      manual == true
    end

    def scheduled?
      !manual?
    end
  end
  ImportJobConfig = Struct.new(:name, :type, :provider, :ticker, :option_root, :paths, :force, keyword_init: true)

  class Config
    attr_reader :timezone, :sqlite_path, :providers, :default_provider_name, :data_dir, :history_dir, :options_dir, :max_workers,
                :retry_count, :retry_delay_seconds, :option_fetch_timeout_seconds, :candle_fetch_timeout_seconds, :jobs, :import_jobs,
                :option_root_tickers, :option_snapshot_filename_timezone, :archives, :universes

    def initialize(
      timezone:,
      sqlite_path:,
      providers:,
      default_provider_name:,
      option_root_tickers:,
      option_snapshot_filename_timezone: "utc",
      archives: nil,
      s3_archive: nil,
      universes: {},
      data_dir:,
      history_dir:,
      options_dir:,
      max_workers:,
      retry_count:,
      retry_delay_seconds:,
      option_fetch_timeout_seconds:,
      candle_fetch_timeout_seconds:,
      jobs:,
      import_jobs:
    )
      @timezone = timezone
      @sqlite_path = sqlite_path
      @providers = providers
      @default_provider_name = default_provider_name
      @option_root_tickers = option_root_tickers
      @option_snapshot_filename_timezone = option_snapshot_filename_timezone
      @archives = normalize_archives(archives: archives, s3_archive: s3_archive)
      @universes = universes
      @data_dir = data_dir
      @history_dir = history_dir
      @options_dir = options_dir
      @max_workers = max_workers
      @retry_count = retry_count
      @retry_delay_seconds = retry_delay_seconds
      @option_fetch_timeout_seconds = option_fetch_timeout_seconds
      @candle_fetch_timeout_seconds = candle_fetch_timeout_seconds
      @jobs = jobs
      @import_jobs = import_jobs
    end

    def job(name)
      selected_name = name.to_s
      selected_job = jobs.find { |candidate| candidate.name == selected_name }
      raise ConfigError, "Unknown job `#{selected_name}`." unless selected_job

      selected_job
    end

    def universe(name)
      selected_name = name.to_s
      selected_universe = @universes.fetch(selected_name, nil)
      raise ConfigError, "Unknown universe `#{selected_name}`." unless selected_universe

      selected_universe
    end

    def import_job(name)
      selected_name = name.to_s
      selected_job = import_jobs.find { |candidate| candidate.name == selected_name }
      raise ConfigError, "Unknown import job `#{selected_name}`." unless selected_job

      selected_job
    end

    def jobs_by_type(type)
      jobs.select { |job| job.type == type.to_s }
    end

    def candles_universe
      jobs_by_type("candles").flat_map(&:universe)
    end

    def candle_lookback_days
      jobs_by_type("candles").first&.lookback_days
    end

    def eod_run_at
      jobs_by_type("candles").first&.run_at
    end

    def eod_days
      jobs_by_type("candles").first&.days || []
    end

    def dte_buckets
      jobs_by_type("options").first&.dte_buckets || []
    end

    def options_windows
      jobs_by_type("options").first&.windows || []
    end

    def options_monitor_interval_seconds
      jobs_by_type("options").first&.interval_seconds
    end

    def options_universe
      jobs_by_type("options").flat_map(&:universe)
    end

    def provider_definition(name = nil)
      selected_name = (name || default_provider_name).to_s
      provider = @providers.fetch(selected_name, nil)
      raise ConfigError, "Unknown provider `#{selected_name}`." unless provider

      provider
    end

    def provider_name_for_entry(entry, scheduled_job: nil, fallback: nil)
      (fallback || scheduled_job&.provider || default_provider_name).to_s
    end

    def provider_name_for_entry_with_override(override_name, entry, scheduled_job: nil)
      return override_name.to_s if override_name

      provider_name_for_entry(entry, scheduled_job: scheduled_job)
    end

    def provider_names_for_job(job, override_name: nil)
      case job.type
      when "options", "candles"
        [provider_name_for_entry_with_override(override_name, nil, scheduled_job: job)].compact.uniq
      when "maintenance"
        explicit_providers = Array(job.tasks).filter_map(&:provider)
        fallback_provider = provider_name_for_entry_with_override(override_name, nil, scheduled_job: job)
        (explicit_providers + [fallback_provider]).compact.uniq
      else
        [provider_name_for_entry_with_override(override_name, nil, scheduled_job: job)].compact.uniq
      end
    end

    def ticker_for_option_root(option_root)
      normalized_root = option_root.to_s.upcase
      @option_root_tickers.fetch(normalized_root, normalized_root)
    end

    def s3_archive
      @archives["s3_archive"]
    end

    private

    def normalize_archives(archives:, s3_archive:)
      normalized = (archives || {}).dup
      normalized["s3_archive"] ||= s3_archive if s3_archive
      normalized
    end
  end
end
