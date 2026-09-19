# frozen_string_literal: true

require "csv"

module Tickrake
  class ChartStreamJob
    HEADERS = %w[datetime open high low close volume].freeze

    CHART_EQUITY_FIELDS = {
      open: "1", high: "2", low: "3", close: "4", volume: "5", sequence: "6", chart_time_ms: "7"
    }.freeze

    CHART_FUTURES_FIELDS = {
      chart_time_ms: "1", open: "2", high: "3", low: "4", close: "5", volume: "6"
    }.freeze

    SERVICE_FIELDS = {
      "CHART_EQUITY"  => CHART_EQUITY_FIELDS,
      "CHART_FUTURES" => CHART_FUTURES_FIELDS
    }.freeze

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @chart_stream_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stop_requested = false
      @buffer = {}
      @buffer_mutex = Mutex.new
      @storage_paths = Storage::Paths.new(runtime.config)
    end

    def run_session(window_start:)
      @stop_requested = false

      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)
      symbols = @scheduled_job.universe

      @chart_stream_config.services.each do |service_sym|
        service_str = SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.fetch(service_sym)
        stream.on(service_sym, symbols: symbols, fields: :all) do |event|
          handle_event(event, service: service_str)
        end
      end

      flush_interval = @chart_stream_config.flush_interval_seconds
      @flush_thread = Thread.new do
        until @stop_requested
          sleep(flush_interval)
          flush_buffer
        end
      rescue StandardError => e
        @runtime.logger.error({ msg: "[chart_stream:#{@job_name}] Flush thread error: #{e.class}: #{e.message}", event: "flush_error" })
      end

      stream.start_async
      wait_for_stop
    ensure
      stream&.stop rescue nil
      flush_buffer
      @flush_thread&.join(10)
    end

    def stop
      @stop_requested = true
    end

    def close; end

    private

    def handle_event(event, service:)
      fields = SERVICE_FIELDS[service]
      return unless fields

      entries = Array(event["content"] || event[:content] || [event])

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        next unless symbol

        chart_time_ms = entry[fields[:chart_time_ms]]&.to_i
        next unless chart_time_ms && chart_time_ms > 0

        bar = Data::Bar.new(
          datetime: Time.at(chart_time_ms / 1000.0).utc,
          open: entry[fields[:open]]&.to_f,
          high: entry[fields[:high]]&.to_f,
          low: entry[fields[:low]]&.to_f,
          close: entry[fields[:close]]&.to_f,
          volume: entry[fields[:volume]]&.to_i,
          source: @provider,
          symbol: symbol.to_s,
          frequency: "1min"
        )

        @buffer_mutex.synchronize do
          (@buffer[symbol.to_s] ||= []) << bar
        end
      end
    end

    def flush_buffer
      bars_by_symbol = @buffer_mutex.synchronize do
        snapshot = @buffer.dup
        @buffer.clear
        snapshot
      end

      return if bars_by_symbol.empty?

      bars_by_symbol.each do |symbol, bars|
        next if bars.empty?

        path = @storage_paths.candle_path(provider: @provider, symbol: symbol, frequency: "1min")
        FileUtils.mkdir_p(File.dirname(path))

        write_headers = !File.exist?(path)
        CSV.open(path, "a") do |csv|
          csv << HEADERS if write_headers
          bars.each do |bar|
            csv << [
              bar.utc_datetime.iso8601,
              bar.open,
              bar.high,
              bar.low,
              bar.close,
              bar.volume
            ]
          end
        end

        @runtime.logger.info("[chart_stream:#{@job_name}] Flushed #{bars.size} bar(s) for #{symbol}")
      end
    end

    def wait_for_stop
      until @stop_requested
        sleep(0.25)
      end
    end
  end
end
