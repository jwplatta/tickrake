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
        bars, path = Timeout.timeout(@runtime.config.candle_fetch_timeout_seconds) do
          fetched_bars = provider.fetch_bars(
            symbol: entry.symbol,
            frequency: frequency,
            start_date: start_date,
            end_date: end_date,
            extended_hours: entry.need_extended_hours_data,
            previous_close: entry.need_previous_close
          )
          output_path = candle_reconciler.write(
            path: storage_paths.candle_path(provider: provider.provider_name, symbol: entry.symbol, frequency: frequency),
            bars: fetched_bars
          )
          [fetched_bars, output_path]
        end
        candle_count = Array(bars).size
        @runtime.logger.info(
          "Wrote #{frequency} candles for #{entry.symbol} to #{path} (requested #{start_date.iso8601} to #{end_date.iso8601}, #{candle_count} rows)"
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
      return entry.start_date unless File.exist?(history_path(entry, frequency))

      lookback_start = scheduled_for.to_date - @runtime.config.candle_lookback_days
      [entry.start_date, lookback_start].max
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
  end
end
