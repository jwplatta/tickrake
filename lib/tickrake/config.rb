# frozen_string_literal: true

module Tickrake
  SchedulerWindow = Struct.new(:days, :start_time, :end_time, keyword_init: true)
  OptionSymbol = Struct.new(:symbol, :option_root, :provider, keyword_init: true)
  ProviderDefinition = Struct.new(:name, :adapter, :settings, :symbol_map, keyword_init: true)
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
    :interval_seconds,
    :windows,
    :run_at,
    :days,
    :lookback_days,
    :dte_buckets,
    :universe,
    keyword_init: true
  ) do
    def options?
      type == "options"
    end

    def candles?
      type == "candles"
    end
  end

  class Config
    attr_reader :timezone, :sqlite_path, :providers, :default_provider_name, :data_dir, :history_dir, :options_dir, :max_workers,
                :retry_count, :retry_delay_seconds, :option_fetch_timeout_seconds, :candle_fetch_timeout_seconds, :jobs

    def initialize(
      timezone:,
      sqlite_path:,
      providers:,
      default_provider_name:,
      data_dir:,
      history_dir:,
      options_dir:,
      max_workers:,
      retry_count:,
      retry_delay_seconds:,
      option_fetch_timeout_seconds:,
      candle_fetch_timeout_seconds:,
      jobs:
    )
      @timezone = timezone
      @sqlite_path = sqlite_path
      @providers = providers
      @default_provider_name = default_provider_name
      @data_dir = data_dir
      @history_dir = history_dir
      @options_dir = options_dir
      @max_workers = max_workers
      @retry_count = retry_count
      @retry_delay_seconds = retry_delay_seconds
      @option_fetch_timeout_seconds = option_fetch_timeout_seconds
      @candle_fetch_timeout_seconds = candle_fetch_timeout_seconds
      @jobs = jobs
    end

    def job(name)
      selected_name = name.to_s
      selected_job = jobs.find { |candidate| candidate.name == selected_name }
      raise ConfigError, "Unknown job `#{selected_name}`." unless selected_job

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

    def provider_name_for_entry(entry, fallback: default_provider_name)
      (entry.provider || fallback).to_s
    end

    def provider_name_for_entry_with_override(override_name, entry)
      return override_name.to_s if override_name

      provider_name_for_entry(entry)
    end
  end
end
