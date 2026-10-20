# frozen_string_literal: true

require "monitor"
require "fileutils"
require "json"

module Tickrake
  module Streaming
    class EventStreamWriter
      DEFAULT_PROGRESS_LOG_INTERVAL = 30 # seconds

      attr_reader :stats

      def initialize(config:, logger:, dataset_type:, provider:, provider_definition: nil, log_interval: DEFAULT_PROGRESS_LOG_INTERVAL)
        @config = config
        @logger = logger
        @dataset_type = dataset_type.to_s
        @provider = provider.to_s
        @provider_definition = provider_definition
        @paths = Tickrake::Storage::Paths.new(config)
        @log_interval = log_interval
        @lock = Monitor.new
        @open_files = {} # [date_str, symbol] => File
        @stats = {} # [date_str, symbol] => { event_count: int, byte_count: int, last_logged_bytes: int, last_logged_events: int, last_logged_at: Time }
      end

      def write(event_hash)
        symbol = event_hash["symbol"]
        return unless symbol

        now = Time.now.utc
        date_str = now.strftime("%Y-%m-%d")
        payload = JSON.generate(event_hash) + "\n"
        payload_bytes = payload.bytesize

        @lock.synchronize do
          file = get_or_open_file(date_str, symbol.to_s, now)
          file.write(payload)
          file.flush

          record_write_stats(date_str, symbol.to_s, payload_bytes, now)
          maybe_log_progress(date_str, symbol.to_s, now)
        end
      end

      def flush_progress(force: false)
        now = Time.now.utc
        @lock.synchronize do
          @stats.each do |(date_str, symbol), s|
            delta_bytes = s[:byte_count] - s[:last_logged_bytes]
            next if delta_bytes.zero? && !force

            if force || (now - s[:last_logged_at]) >= @log_interval
              emit_progress_log(symbol, date_str, s, now)
            end
          end
        end
      end

      def close
        flush_progress(force: true)
        @lock.synchronize do
          @open_files.each_value do |file|
            file.close rescue nil
          end
          @open_files.clear
        end
      end

      private

      def get_or_open_file(date_str, symbol, now)
        key = [date_str, symbol]
        return @open_files[key] if @open_files[key] && !@open_files[key].closed?

        # Close files from prior dates if date rolled over
        stale_keys = @open_files.keys.select { |d, _| d != date_str }
        stale_keys.each do |stale_key|
          @open_files[stale_key].close rescue nil
          @open_files.delete(stale_key)
        end

        target_path = resolve_target_path(symbol, now)
        FileUtils.mkdir_p(File.dirname(target_path))
        @open_files[key] = File.open(target_path, "a")
      end

      def record_write_stats(date_str, symbol, payload_bytes, now)
        key = [date_str, symbol]
        stat = (@stats[key] ||= {
          event_count: 0,
          byte_count: 0,
          last_logged_bytes: 0,
          last_logged_events: 0,
          last_logged_at: now
        })
        stat[:event_count] += 1
        stat[:byte_count] += payload_bytes
      end

      def maybe_log_progress(date_str, symbol, now)
        key = [date_str, symbol]
        stat = @stats[key]
        return unless stat

        if (now - stat[:last_logged_at]) >= @log_interval
          emit_progress_log(symbol, date_str, stat, now)
        end
      end

      def emit_progress_log(symbol, date_str, stat, now)
        delta_bytes = stat[:byte_count] - stat[:last_logged_bytes]
        delta_events = stat[:event_count] - stat[:last_logged_events]
        stat[:last_logged_bytes] = stat[:byte_count]
        stat[:last_logged_events] = stat[:event_count]
        stat[:last_logged_at] = now

        @logger.info({
          msg: "stream_bytes_written",
          event: "stream_bytes_written",
          dataset_type: @dataset_type,
          provider: @provider,
          symbol: symbol,
          market_date: date_str,
          byte_count: stat[:byte_count],
          delta_bytes: delta_bytes,
          event_count: stat[:event_count],
          delta_events: delta_events,
          logged_at: now.iso8601
        })
      end

      def resolve_target_path(symbol, timestamp)
        case @dataset_type
        when "level_one"
          @paths.level_one_path(
            provider: @provider,
            symbol: symbol,
            flush_start: timestamp,
            provider_definition: @provider_definition
          )
        when "order_book"
          @paths.order_book_path(
            provider: @provider,
            symbol: symbol,
            flush_start: timestamp,
            provider_definition: @provider_definition
          )
        else
          raise Tickrake::Error, "Unsupported streaming dataset type: #{@dataset_type}"
        end
      end
    end
  end
end
