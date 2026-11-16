# frozen_string_literal: true

module Tickrake
  class LevelOneJob
    # Common field indices present across Equities, Futures, and Futures Options.
    COMMON_FIELDS = {
      bid: "1",
      ask: "2",
      last: "3",
      bid_size: "4",
      ask_size: "5",
      volume: "8"
    }.freeze

    # Service-specific field index overrides (including quote_time, trade_time, mark,
    # and shifted price/size indices for LEVELONE_OPTIONS and LEVELONE_FOREX).
    SERVICE_FIELD_INDICES = {
      "LEVELONE_EQUITIES"        => { quote_time: "34", trade_time: "35", mark: "33" },
      "LEVELONE_OPTIONS"         => {
        bid: "2", ask: "3", last: "4", volume: "8",
        bid_size: "16", ask_size: "17",
        mark: "37", quote_time: "38", trade_time: "39"
      },
      "LEVELONE_FUTURES"         => { quote_time: "10", trade_time: "11", mark: "24" },
      "LEVELONE_FUTURES_OPTIONS" => { quote_time: "10", trade_time: "11", mark: "19" },
      "LEVELONE_FOREX"           => {
        volume: "6",
        quote_time: "8", trade_time: "9", mark: "29"
      }
    }.freeze

    # How often the watchdog loop checks liveness (seconds).
    WATCHDOG_POLL_SECONDS = 10

    # Default stale threshold — reconnect if no event received within this window.
    DEFAULT_STALE_TIMEOUT_SECONDS = 60

    # Reconnect backoff: initial delay, multiplier, and cap (seconds).
    RECONNECT_INITIAL_DELAY = 5
    RECONNECT_BACKOFF_MULTIPLIER = 2
    RECONNECT_MAX_DELAY = 120
    RECONNECT_MAX_ATTEMPTS = 10

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @level_one_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stale_timeout = (@level_one_config.respond_to?(:stale_timeout_seconds) &&
                        @level_one_config.stale_timeout_seconds) ||
                       DEFAULT_STALE_TIMEOUT_SECONDS

      @stop_requested = false
      @last_event_at  = nil
      @last_event_mu  = Mutex.new
      @subscribed_symbols = []
      @last_resolved_at = nil
      @resolver = nil

      provider_def = runtime.config.provider_definition(@provider)
      @events_writer = Tickrake::Streaming::EventStreamWriter.new(
        config: runtime.config,
        logger: runtime.logger,
        dataset_type: "level_one",
        provider: @provider,
        provider_definition: provider_def
      )
    end

    def run_session(window_start:)
      @stop_requested = false
      attempts = 0

      loop do
        break if @stop_requested

        attempts += 1
        stream = nil

        begin
          stream = build_stream
          @last_event_mu.synchronize { @last_event_at = Time.now }

          stream.start_async
          logger.info({ msg: "#{log_prefix} Stream connected.", event: "stream_connect", attempt: attempts })

          watchdog(stream)
        rescue => e
          logger.error({
            msg: "#{log_prefix} Stream error: #{e.class}: #{e.message}",
            event: "session_error",
            error_class: e.class.name,
            error_message: e.message,
            backtrace: Array(e.backtrace).first(5).join(" | ")
          })
        ensure
          stream&.stop rescue nil
          @events_writer.close
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
    end

    def close; end

    private

    # Build and subscribe a fresh stream client.
    def build_stream
      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)

      if @level_one_config.respond_to?(:options_contracts?) && @level_one_config.options_contracts?
        @resolver = Tickrake::Streaming::OptionContractResolver.new(client, @level_one_config.contracts)
        @subscribed_symbols = @resolver.resolve.dup
        @last_resolved_at = Time.now
        logger.info("#{log_prefix} Resolved #{@subscribed_symbols.size} initial contracts: #{@subscribed_symbols.first(5).join(", ")}...")
      else
        @resolver = nil
        @subscribed_symbols = @scheduled_job.universe.dup
      end

      @level_one_config.services.each do |service_sym|
        service_str = SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.fetch(service_sym)
        stream.on(service_sym, symbols: @subscribed_symbols, fields: :all) do |event|
          handle_event(event, service: service_str)
        end
      end

      stream
    end

    # Block until a stop is requested or the stream goes stale, then return.
    # Logs a +stream_stale+ event and returns (causing the caller to reconnect)
    # if no event arrives within @stale_timeout seconds.
    def watchdog(stream)
      stats_log_interval = 30
      last_stats_log = Time.now

      loop do
        sleep(WATCHDOG_POLL_SECONDS)

        break if @stop_requested

        if (Time.now - last_stats_log) >= stats_log_interval
          @events_writer&.flush_progress
          last_stats_log = Time.now
        end

        if @resolver && @last_resolved_at &&
           (Time.now - @last_resolved_at) >= @level_one_config.re_resolve_interval_seconds
          begin
            new_symbols = @resolver.resolve
            added = new_symbols - @subscribed_symbols
            unless added.empty?
              logger.info("#{log_prefix} Adding #{added.size} new contracts from re-resolution.")
              @level_one_config.services.each do |service_sym|
                stream.add(service_sym, symbols: added, fields: :all)
              end
              @subscribed_symbols |= added
            end
          rescue => e
            logger.error({
              msg: "#{log_prefix} Failed to re-resolve contracts: #{e.class}: #{e.message}",
              event: "contract_resolution_error",
              error_class: e.class.name,
              error_message: e.message
            })
          ensure
            @last_resolved_at = Time.now
          end
        end

        last = @last_event_mu.synchronize { @last_event_at }
        elapsed = last ? (Time.now - last) : @stale_timeout + 1

        if elapsed > @stale_timeout
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

    # Each Level One event carries up to three distinct timestamps:
    # - received_at: local collector receipt time (UTC ISO 8601). Safest cutoff for
    #   "what data did I have by time T?".
    # - quote_time: timestamp attached to the quote information (bid/ask). Note: small
    #   timing reversals relative to received_at can occur due to independent clocks/stages.
    # - trade_time: timestamp attached to the last-trade info. Nil indicates absent trade info
    #   in that message, not that the symbol never traded.
    def handle_event(event, service:)
      received_at = Time.now.utc.iso8601(3)
      @last_event_mu.synchronize { @last_event_at = Time.now }

      entries = Array(event["content"] || event[:content] || [event])

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        next unless symbol

        fields = extract_fields(entry, service)
        next if fields[:bid].nil? && fields[:ask].nil? && fields[:last].nil? &&
                fields[:volume].nil? && fields[:mark].nil? &&
                fields[:bid_size].nil? && fields[:ask_size].nil? &&
                fields[:extra_json].nil?

        @events_writer.write(
          "job_type"   => "level_one",
          "provider"   => @provider,
          "symbol"     => symbol.to_s,
          "service"    => service,
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
      end
    rescue => e
      logger.error({
        msg: "#{log_prefix} Error handling #{service} event: #{e.class}: #{e.message}",
        event: "event_handler_error",
        service: service,
        error_class: e.class.name,
        error_message: e.message
      })
    end

    def extract_fields(entry, service)
      indices = SERVICE_FIELD_INDICES[service] || {}
      merged  = COMMON_FIELDS.merge(indices)

      bid       = entry[merged[:bid]]&.to_f
      ask       = entry[merged[:ask]]&.to_f
      last      = entry[merged[:last]]&.to_f
      bid_size  = entry[merged[:bid_size]]&.to_i
      ask_size  = entry[merged[:ask_size]]&.to_i
      volume    = entry[merged[:volume]]&.to_i
      quote_time_raw = merged[:quote_time] ? entry[merged[:quote_time]] : nil
      quote_time = quote_time_raw ? Time.at(quote_time_raw.to_i / 1000.0).utc.iso8601(3) : nil
      trade_time_raw = merged[:trade_time] ? entry[merged[:trade_time]] : nil
      trade_time = trade_time_raw ? Time.at(trade_time_raw.to_i / 1000.0).utc.iso8601(3) : nil
      mark          = merged[:mark]        ? entry[merged[:mark]]&.to_f        : nil

      extracted_keys = (merged.values + ["key"]).uniq
      extra = entry.reject { |k, _| extracted_keys.include?(k.to_s) }
      extra_json = extra.empty? ? nil : JSON.dump(extra)

      {
        bid: bid, ask: ask, last: last,
        bid_size: bid_size, ask_size: ask_size, volume: volume,
        quote_time: quote_time, trade_time: trade_time,
        mark: mark, extra_json: extra_json
      }
    end

    def logger
      @runtime.logger
    end

    def log_prefix
      "[level_one:#{@job_name}]"
    end
  end
end
