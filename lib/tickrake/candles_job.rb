# frozen_string_literal: true

module Tickrake
  class CandlesJob
    def initialize(runtime)
      @runtime = runtime
    end

    def run(now: Time.now)
      client = @runtime.client_factory.build
      @runtime.config.candles_universe.each do |entry|
        fetch_one(entry, client, now)
      end
    end

    private

    def fetch_one(entry, client, scheduled_for)
      id = @runtime.tracker.record_start(
        job_type: "eod_candles",
        dataset_type: "candles",
        symbol: entry.symbol,
        scheduled_for: scheduled_for,
        started_at: Time.now
      )

      retries = 0
      begin
        request = Tickrake::Serializers.price_history_request(entry.frequency)
        response = Timeout.timeout(@runtime.config.candle_fetch_timeout_seconds) do
          client.get_price_history(
            entry.symbol,
            period_type: request.fetch(:period_type),
            period: request.fetch(:period),
            frequency_type: request.fetch(:frequency_type),
            frequency: request.fetch(:frequency),
            start_datetime: entry.start_date.to_time,
            end_datetime: Time.now,
            need_extended_hours_data: entry.need_extended_hours_data,
            need_previous_close: entry.need_previous_close,
            return_data_objects: false
          )
        end
        path = Tickrake::Serializers.history_path(
          directory: @runtime.config.history_dir,
          symbol: entry.symbol,
          frequency: entry.frequency
        )
        Tickrake::Serializers.merge_history(path, response)
        @runtime.tracker.record_finish(id: id, status: "success", finished_at: Time.now, output_path: path)
      rescue StandardError => e
        retries += 1
        if retries <= @runtime.config.retry_count
          sleep @runtime.config.retry_delay_seconds
          retry
        end
        @runtime.tracker.record_finish(id: id, status: "failed", finished_at: Time.now, error_message: e.message)
      end
    end
  end
end
