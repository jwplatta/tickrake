# frozen_string_literal: true

module Tickrake
  class ConfigLoader
    DEFAULT_DTE_BUCKETS = %w[
      0DTE 1DTE 2DTE 3DTE 4DTE 5DTE 6DTE 7DTE 8DTE 9DTE 10DTE 30DTE
    ].freeze
    VALID_DAYS = %w[mon tue wed thu fri sat sun].freeze

    def self.load(path)
      new(path).load
    end

    def initialize(path)
      @path = File.expand_path(path)
    end

    def load
      data = YAML.safe_load(File.read(@path), permitted_classes: [Date], aliases: true) || {}

      timezone = data.fetch("timezone", ENV.fetch("TZ", "America/Chicago"))
      sqlite_path = Tickrake::PathSupport.expand_path(data.fetch("sqlite_path", "~/.schwab_rb/data/tickrake.sqlite3"))
      history_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "history_dir", "~/.schwab_rb/data/history"))
      options_dir = Tickrake::PathSupport.expand_path(dig(data, "storage", "options_dir", "~/.schwab_rb/data/options"))

      runtime = data.fetch("runtime", {})
      schedule = data.fetch("schedule", {})
      options_schedule = schedule.fetch("options_monitor", {})
      eod_schedule = schedule.fetch("eod_candles", {})

      options_windows = Array(options_schedule.fetch("windows", [])).map do |window|
        SchedulerWindow.new(
          days: normalize_days(window.fetch("days")),
          start_time: normalize_clock(window.fetch("start")),
          end_time: normalize_clock(window.fetch("end"))
        )
      end

      dte_buckets = Array(dig(data, "options", "dte_buckets", DEFAULT_DTE_BUCKETS)).map do |bucket|
        parse_bucket(bucket)
      end

      options_universe = Array(dig(data, "options", "universe", [])).map do |row|
        OptionSymbol.new(symbol: row.fetch("symbol"), option_root: row["option_root"])
      end

      candles_universe = Array(dig(data, "candles", "universe", [])).map do |row|
        CandleSymbol.new(
          symbol: row.fetch("symbol"),
          frequency: normalize_frequency(row.fetch("frequency", "day")),
          start_date: Date.iso8601(row.fetch("start_date")),
          need_extended_hours_data: !!row.fetch("need_extended_hours_data", false),
          need_previous_close: !!row.fetch("need_previous_close", false)
        )
      end

      config = Config.new(
        timezone: timezone,
        sqlite_path: sqlite_path,
        history_dir: history_dir,
        options_dir: options_dir,
        max_workers: Integer(runtime.fetch("max_workers", 4)),
        retry_count: Integer(runtime.fetch("retry_count", 2)),
        retry_delay_seconds: Integer(runtime.fetch("retry_delay_seconds", 2)),
        option_fetch_timeout_seconds: Integer(runtime.fetch("option_fetch_timeout_seconds", 30)),
        candle_fetch_timeout_seconds: Integer(runtime.fetch("candle_fetch_timeout_seconds", 60)),
        options_monitor_interval_seconds: Integer(options_schedule.fetch("interval_seconds", 300)),
        options_windows: options_windows,
        eod_run_at: normalize_clock(eod_schedule.fetch("run_at", "16:10")),
        eod_days: normalize_days(eod_schedule.fetch("days", %w[mon tue wed thu fri])),
        dte_buckets: dte_buckets,
        options_universe: options_universe,
        candles_universe: candles_universe
      )

      validate!(config)
      config
    rescue Errno::ENOENT => e
      raise ConfigError, "Config file not found: #{e.message}"
    rescue KeyError, ArgumentError, TypeError, Date::Error, Psych::Exception => e
      raise ConfigError, e.message
    end

    private

    def validate!(config)
      raise ConfigError, "At least one options monitor window is required." if config.options_windows.empty?
      raise ConfigError, "At least one options universe symbol is required." if config.options_universe.empty?
      raise ConfigError, "At least one candle universe symbol is required." if config.candles_universe.empty?
      raise ConfigError, "options_monitor interval must be positive." if config.options_monitor_interval_seconds <= 0
      raise ConfigError, "max_workers must be positive." if config.max_workers <= 0
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
      normalized = value.to_s.downcase
      return normalized if %w[1min 5min 10min 15min 30min day week month].include?(normalized)

      raise ConfigError, "Unsupported candle frequency: #{value}"
    end
  end
end
