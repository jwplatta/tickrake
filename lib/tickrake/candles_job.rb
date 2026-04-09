# frozen_string_literal: true

module Tickrake
  class CandlesJob
    def initialize(runtime, from_config_start: false)
      @runtime = runtime
      @from_config_start = from_config_start
    end

    def run(now: Time.now)
      @runtime.logger.info("Starting candle scrape at #{now.utc.iso8601}")
      client = @runtime.client_factory.build
      @runtime.config.candles_universe.each do |entry|
        entry.frequencies.each do |frequency|
          fetch_one(entry, frequency, client, now)
        end
      end
      @runtime.logger.info("Completed candle scrape at #{Time.now.utc.iso8601}")
    end

    private

    def fetch_one(entry, frequency, client, scheduled_for)
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
        response, path = Timeout.timeout(@runtime.config.candle_fetch_timeout_seconds) do
          SchwabRb::PriceHistory::Downloader.resolve(
            client: client,
            symbol: entry.symbol,
            start_date: start_date,
            end_date: end_date,
            directory: @runtime.config.history_dir,
            frequency: frequency,
            format: "csv",
            need_extended_hours_data: entry.need_extended_hours_data,
            need_previous_close: entry.need_previous_close
          )
        end
        candle_count = Array(response[:candles]).size
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
      SchwabRb::PriceHistory::Downloader.canonical_output_path(
        directory: @runtime.config.history_dir,
        symbol: entry.symbol,
        frequency: frequency,
        format: "csv"
      )
    end
  end
end
