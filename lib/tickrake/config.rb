# frozen_string_literal: true

module Tickrake
  SchedulerWindow = Struct.new(:days, :start_time, :end_time, keyword_init: true)
  OptionSymbol = Struct.new(:symbol, :option_root, keyword_init: true)
  CandleSymbol = Struct.new(
    :symbol,
    :frequencies,
    :start_date,
    :need_extended_hours_data,
    :need_previous_close,
    keyword_init: true
  )

  class Config
    attr_reader :timezone, :sqlite_path, :history_dir, :options_dir, :max_workers,
                :retry_count, :retry_delay_seconds, :option_fetch_timeout_seconds,
                :candle_fetch_timeout_seconds, :options_monitor_interval_seconds,
                :options_windows, :eod_run_at, :eod_days, :candle_lookback_days, :dte_buckets,
                :options_universe, :candles_universe

    def initialize(
      timezone:,
      sqlite_path:,
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
  end
end
