# frozen_string_literal: true

module Tickrake
  class CandlesJob
    def initialize(runtime, from_config_start: false)
      @runtime = runtime
      @from_config_start = from_config_start
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting candle scrape at #{now.utc.iso8601}")
      provider = @runtime.provider_factory.build
      @runtime.config.candles_universe.each do |entry|
        entry.frequencies.each do |frequency|
          fetch_one(entry, frequency, provider, now)
        end
      end
      @runtime.logger.info("Completed candle scrape at #{Time.now.utc.iso8601}")
    end

    private

    def fetch_one(entry, frequency, provider, scheduled_for)
      @runtime.logger.info("Fetching #{frequency} candles for #{entry.symbol}")
      id = @runtime.tracker.record_start(
        job_type: "eod_candles",
        dataset_type: "candles",
        symbol: entry.symbol,
        frequency: frequency,
        scheduled_for: scheduled_for,
        started_at: Time.now
      )

      retries = 0
      begin
        start_date = request_start_date(entry, frequency, scheduled_for)
        end_date = scheduled_for.to_date + 1
        path = storage_paths.candle_path(provider: provider.provider_name, symbol: entry.symbol, frequency: frequency)
        ranges = request_ranges(provider: provider, frequency: frequency, start_date: start_date, end_date: end_date)
        total_candles = 0

        ranges.each_with_index do |(chunk_start, chunk_end), index|
          @runtime.logger.info(
            "Fetching #{frequency} candle chunk #{index + 1}/#{ranges.length} for #{entry.symbol} (#{chunk_start.iso8601} to #{chunk_end.iso8601})"
          ) if ranges.length > 1

          fetched_bars = Timeout.timeout(@runtime.config.candle_fetch_timeout_seconds) do
            provider.fetch_bars(
              symbol: entry.symbol,
              frequency: frequency,
              start_date: chunk_start,
              end_date: chunk_end,
              extended_hours: entry.need_extended_hours_data,
              previous_close: entry.need_previous_close
            )
          end
          candle_reconciler.write(path: path, bars: fetched_bars)
          total_candles += Array(fetched_bars).size
        end

        @runtime.logger.info(
          "Wrote #{frequency} candles for #{entry.symbol} to #{path} (requested #{start_date.iso8601} to #{end_date.iso8601}, #{total_candles} rows)"
        )
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          @runtime.logger.warn("Retry #{retries} for #{entry.symbol} #{frequency}: #{e.message}")
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.logger.error("Failed candle fetch for #{entry.symbol} #{frequency}: #{e.message}")
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
      end
    end

    def request_start_date(entry, frequency, scheduled_for)
      return entry.start_date if @from_config_start
      return lookback_start_date(entry, scheduled_for) if ibkr_intraday_frequency?(frequency)
      return entry.start_date unless File.exist?(history_path(entry, frequency))

      lookback_start_date(entry, scheduled_for)
    end

    def history_path(entry, frequency)
      storage_paths.candle_path(provider: @runtime.config.provider, symbol: entry.symbol, frequency: frequency)
    end

    def storage_paths
      @storage_paths ||= Storage::Paths.new(@runtime.config)
    end

    def candle_reconciler
      @candle_reconciler ||= Storage::CandleReconciler.new
    end

    def lookback_start_date(entry, scheduled_for)
      lookback_start = scheduled_for.to_date - @runtime.config.candle_lookback_days
      [entry.start_date, lookback_start].max
    end

    def ibkr_intraday_frequency?(frequency)
      @runtime.config.provider == "ibkr" && !%w[day week month].include?(frequency)
    end

    def request_ranges(provider:, frequency:, start_date:, end_date:)
      chunk_days = provider_chunk_days(provider: provider, frequency: frequency)
      return [[start_date, end_date]] unless chunk_days

      ranges = []
      cursor = start_date
      while cursor <= end_date
        chunk_end = [cursor + (chunk_days - 1), end_date].min
        ranges << [cursor, chunk_end]
        cursor = chunk_end + 1
      end
      ranges
    end

    def provider_chunk_days(provider:, frequency:)
      return unless provider.provider_name == "ibkr"

      {
        "1min" => 6,
        "5min" => 6,
        "10min" => 6,
        "15min" => 20,
        "30min" => 34,
        "day" => 365,
        "week" => 365,
        "month" => 365
      }.fetch(frequency, nil)
    end
  end
end
