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

  class Config
    attr_reader :timezone, :sqlite_path, :providers, :default_provider_name, :data_dir, :history_dir, :options_dir, :max_workers,
                :retry_count, :retry_delay_seconds, :option_fetch_timeout_seconds,
                :candle_fetch_timeout_seconds, :options_monitor_interval_seconds,
                :options_windows, :eod_run_at, :eod_days, :candle_lookback_days, :dte_buckets,
                :options_universe, :candles_universe

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
      options_monitor_interval_seconds:,
      options_windows:,
      eod_run_at:,
      eod_days:,
      candle_lookback_days:,
      dte_buckets:,
      options_universe:,
      candles_universe:
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
      @options_monitor_interval_seconds = options_monitor_interval_seconds
      @options_windows = options_windows
      @eod_run_at = eod_run_at
      @eod_days = eod_days
      @candle_lookback_days = candle_lookback_days
      @dte_buckets = dte_buckets
      @options_universe = options_universe
      @candles_universe = candles_universe
    end

    def provider_definition(name = nil)
      selected_name = provider_name_for(name)
      provider = @providers.fetch(selected_name, nil)
      raise ConfigError, "Unknown provider `#{selected_name}`." unless provider

      provider
    end

    def provider_name_for(name_or_entry = nil, fallback: default_provider_name)
      raw_name =
        if name_or_entry.respond_to?(:provider)
          name_or_entry.provider
        else
          name_or_entry
        end

      (raw_name || fallback).to_s
    end

    def provider_name_with_override(override_name, entry = nil)
      provider_name_for(override_name || entry)
    end
  end
end
