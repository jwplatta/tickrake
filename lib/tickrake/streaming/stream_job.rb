# frozen_string_literal: true

require "csv"

module Tickrake
  class StreamJob
    WATCHDOG_POLL_SECONDS = 10
    DEFAULT_STALE_TIMEOUT_SECONDS = 60
    RECONNECT_INITIAL_DELAY = 5
    RECONNECT_BACKOFF_MULTIPLIER = 2
    RECONNECT_MAX_DELAY = 120
    RECONNECT_MAX_ATTEMPTS = 10

    CHART_HEADERS = %w[datetime open high low close volume].freeze

    attr_reader :service_stats

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @stream_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stale_timeout = @stream_config.stale_timeout_seconds || DEFAULT_STALE_TIMEOUT_SECONDS

      @stop_requested = false
      @last_event_at = nil
      @last_event_mu = Mutex.new

      @writers = {}
      @writers_mu = Mutex.new

      @chart_buffers = {}
      @chart_buffer_mu = Mutex.new
      @storage_paths = Storage::Paths.new(runtime.config)

      @active_subscriptions = {} # name => Set<symbols>
      @active_mu = Mutex.new
      @stream = nil

      @stats_mu = Mutex.new
      @service_stats = Hash.new do |h, k|
        h[k] = {
          callbacks: 0,
          entries: 0,
          accepted: 0,
          discarded: 0,
          last_event_at: nil
        }
      end
    end

    def run_session(window_start:)
      @stop_requested = false
      attempts = 0

      loop do
        break if @stop_requested

        attempts += 1
        @stream = nil
        @active_mu.synchronize { @active_subscriptions.clear }

        begin
          @stream = build_stream_client
          @last_event_mu.synchronize { @last_event_at = Time.now }

          # Initial subscription for all currently in-window subscriptions
          sync_subscriptions(Time.now)

          @stream.start_async
          logger.info({ msg: "#{log_prefix} Stream connected.", event: "stream_connect", attempt: attempts })

          watchdog(@stream)
        rescue => e
          logger.error({
            msg: "#{log_prefix} Stream error: #{e.class}: #{e.message}",
            event: "session_error",
            error_class: e.class.name,
            error_message: e.message,
            backtrace: Array(e.backtrace).first(5).join(" | ")
          })
        ensure
          @stream&.stop rescue nil
          @active_mu.synchronize { @active_subscriptions.clear }
          close_writers_and_flush
        end

        break if @stop_requested

        if attempts >= RECONNECT_MAX_ATTEMPTS
          logger.error({ msg: "#{log_prefix} Exceeded max reconnect attempts (#{RECONNECT_MAX_ATTEMPTS}), giving up.", event: "session_error" })
          break
        end

        delay = [RECONNECT_INITIAL_DELAY * (RECONNECT_BACKOFF_MULTIPLIER**(attempts - 1)), RECONNECT_MAX_DELAY].min
        logger.info({ msg: "#{log_prefix} Reconnecting in #{delay}s (attempt #{attempts}).", event: "reconnect_attempt", delay: delay, attempt: attempts })
        sleep(delay) unless @stop_requested
      end
    end

    def stop
      @stop_requested = true
      @stream&.stop rescue nil
    end

    def close
      close_writers_and_flush
    end

    # Called periodically by StreamRunner to dynamically ADD or UNSUBS symbols as windows shift
    def sync_subscriptions(now = Time.now)
      return unless @stream

      @stream_config.subscriptions.each do |sub|
        in_win = sub.in_window?(now)
        currently_active = @active_mu.synchronize { @active_subscriptions.key?(sub.name) }

        if in_win && !currently_active
          activate_subscription(sub)
        elsif !in_win && currently_active
          deactivate_subscription(sub)
        end
      end
    end

    private

    def build_stream_client
      client = Tickrake::ClientFactory.new(@runtime.config).build
      SchwabRb::Stream::Client.new(client)
    end

    def activate_subscription(sub)
      @active_mu.synchronize { @active_subscriptions[sub.name] = sub.symbols.dup }
      logger.info({ msg: "#{log_prefix} Activating #{sub.kind} subscription '#{sub.name}' for #{sub.symbols.join(", ")}", event: "subscription_add" })

      sub_services = sub.settings.services
      sub_services.each do |service_sym|
        service_str = SchwabRb::Stream::Services::SYMBOL_TO_SERVICE[service_sym] || service_sym.to_s
        stream_service_sym = service_sym.to_s.downcase.to_sym

        @stream.add(stream_service_sym, symbols: sub.symbols, fields: :all) do |event|
          handle_event(event, service_str: service_str, subscription: sub)
        end
      end
    end

    def deactivate_subscription(sub)
      @active_mu.synchronize { @active_subscriptions.delete(sub.name) }
      logger.info({ msg: "#{log_prefix} Deactivating #{sub.kind} subscription '#{sub.name}' for #{sub.symbols.join(", ")}", event: "subscription_unsub" })

      sub_services = sub.settings.services
      sub_services.each do |service_sym|
        stream_service_sym = service_sym.to_s.downcase.to_sym
        @stream.unsub(stream_service_sym, symbols: sub.symbols)
      end

      # Flush or rotate files for this deactivated subscription
      if sub.kind == :chart_stream
        flush_chart_buffer(sub)
      else
        writer = @writers_mu.synchronize { @writers[sub.name] }
        writer&.close
        @writers_mu.synchronize { @writers.delete(sub.name) }
      end
    end

    def watchdog(stream)
      chart_flush_interval = 60
      last_flush = Time.now
      stats_log_interval = 30
      last_stats_log = Time.now

      loop do
        sleep(WATCHDOG_POLL_SECONDS)

        break if @stop_requested

        # Periodic delivery stats logging
        if (Time.now - last_stats_log) >= stats_log_interval
          log_delivery_stats
          flush_all_writers_progress
          last_stats_log = Time.now
        end

        # Periodic chart flush for chart streams
        if (Time.now - last_flush) >= chart_flush_interval
          flush_all_chart_buffers
          last_flush = Time.now
        end

        last = @last_event_mu.synchronize { @last_event_at }
        elapsed = last ? (Time.now - last) : @stale_timeout + 1

        # Only consider stale if we actually have active subscriptions
        has_active = @active_mu.synchronize { !@active_subscriptions.empty? }

        if has_active && elapsed > @stale_timeout
          logger.warn({
            msg: "#{log_prefix} No events for #{elapsed.round}s — stream stale, reconnecting.",
            event: "stream_stale",
            elapsed_seconds: elapsed.round,
            stale_timeout: @stale_timeout
          })
          stream&.stop rescue nil
          break
        end
      end
    end

    def handle_event(event, service_str:, subscription:)
      received_at = Time.now.utc.iso8601
      event_now = Time.now
      @last_event_mu.synchronize { @last_event_at = event_now }

      @stats_mu.synchronize do
        stats = @service_stats[service_str]
        stats[:callbacks] += 1
        stats[:last_event_at] = event_now
      end

      case subscription.kind
      when :level_one
        handle_level_one_event(event, service_str: service_str, subscription: subscription, received_at: received_at)
      when :order_book
        handle_order_book_event(event, service_str: service_str, subscription: subscription, received_at: received_at)
      when :chart_stream
        handle_chart_event(event, service_str: service_str, subscription: subscription)
      end
    end

    # Each Level One event carries up to three distinct timestamps:
    # - received_at: local collector receipt time (UTC ISO 8601). Safest cutoff for
    #   "what data did I have by time T?".
    # - quote_time: timestamp attached to the quote information (bid/ask). Note: small
    #   timing reversals relative to received_at can occur due to independent clocks/stages;
    #   do not assume received_at - quote_time is a precise network latency measurement.
    # - trade_time: timestamp attached to the last-trade info. Nil indicates absent trade info
    #   in that message, not that the symbol never traded.
    def handle_level_one_event(event, service_str:, subscription:, received_at:)
      writer = get_or_create_writer(subscription)
      entries = Array(event["content"] || event[:content] || [event])

      accepted_count = 0
      discarded_count = 0

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        unless symbol
          discarded_count += 1
          next
        end

        fields = extract_level_one_fields(entry, service_str)
        if fields[:bid].nil? && fields[:ask].nil? && fields[:last].nil? &&
           fields[:volume].nil? && fields[:mark].nil? &&
           fields[:bid_size].nil? && fields[:ask_size].nil?
          discarded_count += 1
          next
        end

        writer.write(
          "job_type"   => "level_one",
          "provider"   => @provider,
          "symbol"     => symbol.to_s,
          "service"    => service_str,
          "received_at"=> received_at,
          "quote_time" => fields[:quote_time],
          "trade_time" => fields[:trade_time],
          "bid"        => fields[:bid],
          "ask"        => fields[:ask],
          "last"       => fields[:last],
          "bid_size"   => fields[:bid_size],
          "ask_size"   => fields[:ask_size],
          "volume"     => fields[:volume],
          "mark"       => fields[:mark],
          "extra_json" => fields[:extra_json]
        )
        accepted_count += 1
      end

      record_delivery_counts(service_str, entries_count: entries.size, accepted: accepted_count, discarded: discarded_count)
    end

    def extract_level_one_fields(entry, service)
      indices = LevelOneJob::SERVICE_FIELD_INDICES[service] || {}
      common = LevelOneJob::COMMON_FIELDS

      bid       = entry[common[:bid]]&.to_f
      ask       = entry[common[:ask]]&.to_f
      last      = entry[common[:last]]&.to_f
      bid_size  = entry[common[:bid_size]]&.to_i
      ask_size  = entry[common[:ask_size]]&.to_i
      volume    = entry[common[:volume]]&.to_i
      quote_time_raw = indices[:quote_time] ? entry[indices[:quote_time]] : nil
      quote_time = quote_time_raw ? Time.at(quote_time_raw.to_i / 1000.0).utc.iso8601 : nil
      trade_time_raw = indices[:trade_time] ? entry[indices[:trade_time]] : nil
      trade_time = trade_time_raw ? Time.at(trade_time_raw.to_i / 1000.0).utc.iso8601 : nil
      mark          = indices[:mark]        ? entry[indices[:mark]]&.to_f        : nil

      extracted_keys = (common.values + indices.values + ["key"]).uniq
      extra = entry.reject { |k, _| extracted_keys.include?(k.to_s) }
      extra_json = extra.empty? ? nil : JSON.dump(extra)

      {
        bid: bid, ask: ask, last: last,
        bid_size: bid_size, ask_size: ask_size, volume: volume,
        quote_time: quote_time, trade_time: trade_time,
        mark: mark, extra_json: extra_json
      }
    end

    def handle_order_book_event(event, service_str:, subscription:, received_at:)
      writer = get_or_create_writer(subscription)
      entries = Array(event["content"] || event[:content] || [event])

      accepted_count = 0
      discarded_count = 0

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        unless symbol
          discarded_count += 1
          next
        end

        book_time_raw = entry["1"] || entry[:book_time]
        book_time = book_time_raw ? Time.at(book_time_raw.to_i / 1000.0).utc.iso8601 : nil

        writer.write(
          "job_type"    => "order_book",
          "provider"    => @provider,
          "symbol"      => symbol.to_s,
          "service"     => service_str,
          "received_at" => received_at,
          "book_time"   => book_time,
          "bids_json"   => extract_json(entry["2"] || entry[:bids]),
          "asks_json"   => extract_json(entry["3"] || entry[:asks])
        )
        accepted_count += 1
      end

      record_delivery_counts(service_str, entries_count: entries.size, accepted: accepted_count, discarded: discarded_count)
    end

    def handle_chart_event(event, service_str:, subscription:)
      fields = ChartStreamJob::SERVICE_FIELDS[service_str]
      unless fields
        record_delivery_counts(service_str, entries_count: 0, accepted: 0, discarded: 0)
        return
      end

      entries = Array(event["content"] || event[:content] || [event])

      accepted_count = 0
      discarded_count = 0

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        unless symbol
          discarded_count += 1
          next
        end

        chart_time_ms = entry[fields[:chart_time_ms]]&.to_i
        unless chart_time_ms && chart_time_ms > 0
          discarded_count += 1
          next
        end

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

        @chart_buffer_mu.synchronize do
          buf = (@chart_buffers[subscription.name] ||= {})
          (buf[symbol.to_s] ||= []) << bar
        end
        accepted_count += 1
      end

      record_delivery_counts(service_str, entries_count: entries.size, accepted: accepted_count, discarded: discarded_count)
    end

    def record_delivery_counts(service_str, entries_count:, accepted:, discarded:)
      @stats_mu.synchronize do
        stats = @service_stats[service_str]
        stats[:entries] += entries_count
        stats[:accepted] += accepted
        stats[:discarded] += discarded
      end
    end

    def log_delivery_stats
      stats_snapshot = @stats_mu.synchronize { @service_stats.transform_values(&:dup) }
      stats_snapshot.each do |service_str, s|
        next if s[:callbacks].zero? && s[:entries].zero?

        logger.info({
          msg: "#{log_prefix}[#{service_str}] Stats: callbacks=#{s[:callbacks]} entries=#{s[:entries]} accepted=#{s[:accepted]} discarded=#{s[:discarded]} last_event_at=#{s[:last_event_at]&.iso8601}",
          event: "stream_delivery_stats",
          service: service_str,
          callbacks: s[:callbacks],
          entries: s[:entries],
          accepted: s[:accepted],
          discarded: s[:discarded],
          last_event_at: s[:last_event_at]&.iso8601
        })
      end
    end

    def flush_all_chart_buffers
      @stream_config.subscriptions.select { |s| s.kind == :chart_stream }.each do |sub|
        flush_chart_buffer(sub)
      end
    end

    def flush_chart_buffer(subscription)
      bars_by_symbol = @chart_buffer_mu.synchronize do
        buf = @chart_buffers[subscription.name]
        return if buf.nil? || buf.empty?

        snapshot = buf.dup
        buf.clear
        snapshot
      end

      return if bars_by_symbol.nil? || bars_by_symbol.empty?

      bars_by_symbol.each do |symbol, bars|
        next if bars.empty?

        provider_def = @runtime.config.provider_definition(@provider)
        bars_by_date = bars.group_by { |b| b.utc_datetime.to_date }

        bars_by_date.each do |market_date, date_bars|
          next if date_bars.empty?

          path = @storage_paths.candle_path(
            provider: @provider,
            symbol: symbol,
            frequency: "1min",
            date: market_date,
            provider_definition: provider_def
          )
          FileUtils.mkdir_p(File.dirname(path))

          write_headers = !File.exist?(path)
          csv_payload = CSV.generate do |csv|
            csv << CHART_HEADERS if write_headers
            date_bars.each do |bar|
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
          File.open(path, "ab") { |file| file.write(csv_payload) }

          logged_at = Time.now.utc
          logger.info({
            msg: "stream_bytes_written",
            event: "stream_bytes_written",
            dataset_type: "chart_stream",
            provider: @provider,
            symbol: symbol,
            market_date: market_date.strftime("%Y-%m-%d"),
            delta_events: date_bars.size,
            delta_bytes: csv_payload.bytesize,
            logged_at: logged_at.iso8601
          })
        end
      end
    end

    def get_or_create_writer(subscription)
      @writers_mu.synchronize do
        @writers[subscription.name] ||= begin
          provider_def = @runtime.config.provider_definition(@provider)
          dataset_type = subscription.kind.to_s
          Tickrake::Streaming::EventStreamWriter.new(
            config: @runtime.config,
            logger: @runtime.logger,
            dataset_type: dataset_type,
            provider: @provider,
            provider_definition: provider_def
          )
        end
      end
    end

    def flush_all_writers_progress
      @writers_mu.synchronize do
        @writers.each_value(&:flush_progress)
      end
    end

    def close_writers_and_flush
      @writers_mu.synchronize do
        @writers.each_value(&:close)
        @writers.clear
      end
      flush_all_chart_buffers
    end

    def extract_json(data)
      JSON.dump(data) if data
    end

    def logger
      @runtime.logger
    end

    def log_prefix
      "[stream:#{@job_name}]"
    end
  end
end
