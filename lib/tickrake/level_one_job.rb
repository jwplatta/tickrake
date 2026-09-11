# frozen_string_literal: true

module Tickrake
  class LevelOneJob
    # Common field indices present across all Level 1 services.
    COMMON_FIELDS = { bid: "1", ask: "2", last: "3", bid_size: "4", ask_size: "5", volume: "8" }.freeze

    # Service-specific indices for quote_time_ms, trade_time_ms, and mark.
    SERVICE_FIELD_INDICES = {
      "LEVELONE_EQUITIES"        => { quote_time: "34", trade_time: "35", mark: "33" },
      "LEVELONE_OPTIONS"         => { quote_time: "38", trade_time: "39", mark: "37" },
      "LEVELONE_FUTURES"         => { quote_time: "10", trade_time: "11", mark: "24" },
      "LEVELONE_FUTURES_OPTIONS" => { quote_time: "10", trade_time: "11", mark: "19" },
      "LEVELONE_FOREX"           => { quote_time: "8",  trade_time: "9",  mark: "29" }
    }.freeze

    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @level_one_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stop_requested = false

      @events_writer = Tickrake::EventsWriter.new(
        pending_events_dir: runtime.config.pending_events_dir,
        job_name: @job_name,
        rotation_interval_seconds: @level_one_config.rotation_interval_seconds,
        logger: runtime.logger
      )
    end

    def run_session(window_start:)
      @stop_requested = false

      @events_writer.recover_stale_files

      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)
      symbols = @scheduled_job.universe

      @level_one_config.services.each do |service_sym|
        service_str = SchwabRb::Stream::Services::SYMBOL_TO_SERVICE.fetch(service_sym)
        stream.on(service_sym, symbols: symbols, fields: :all) do |event|
          handle_event(event, service: service_str)
        end
      end

      stream.start_async

      wait_for_stop
    ensure
      stream&.stop rescue nil
      @events_writer.close
    end

    def stop
      @stop_requested = true
    end

    def close; end

    private

    def handle_event(event, service:)
      received_at = (Time.now.to_f * 1000).to_i
      entries = Array(event["content"] || event[:content] || [event])

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        next unless symbol

        fields = extract_fields(entry, service)
        next if fields[:bid].nil? && fields[:ask].nil? && fields[:last].nil? &&
                fields[:volume].nil? && fields[:mark].nil? &&
                fields[:bid_size].nil? && fields[:ask_size].nil?

        @events_writer.write(
          "job_type"      => "level_one",
          "provider"      => @provider,
          "symbol"        => symbol.to_s,
          "service"       => service,
          "received_at"   => received_at,
          "quote_time_ms" => fields[:quote_time_ms],
          "trade_time_ms" => fields[:trade_time_ms],
          "bid"           => fields[:bid],
          "ask"           => fields[:ask],
          "last"          => fields[:last],
          "bid_size"      => fields[:bid_size],
          "ask_size"      => fields[:ask_size],
          "volume"        => fields[:volume],
          "mark"          => fields[:mark],
          "extra_json"    => fields[:extra_json]
        )
      end
    end

    def extract_fields(entry, service)
      indices = SERVICE_FIELD_INDICES[service] || {}

      bid       = entry[COMMON_FIELDS[:bid]]&.to_f
      ask       = entry[COMMON_FIELDS[:ask]]&.to_f
      last      = entry[COMMON_FIELDS[:last]]&.to_f
      bid_size  = entry[COMMON_FIELDS[:bid_size]]&.to_i
      ask_size  = entry[COMMON_FIELDS[:ask_size]]&.to_i
      volume    = entry[COMMON_FIELDS[:volume]]&.to_i
      quote_time_ms = indices[:quote_time] ? entry[indices[:quote_time]]&.to_i : nil
      trade_time_ms = indices[:trade_time] ? entry[indices[:trade_time]]&.to_i : nil
      mark          = indices[:mark]        ? entry[indices[:mark]]&.to_f        : nil

      extracted_keys = (COMMON_FIELDS.values + indices.values + ["key"]).uniq
      extra = entry.reject { |k, _| extracted_keys.include?(k.to_s) }
      extra_json = extra.empty? ? nil : JSON.dump(extra)

      {
        bid: bid, ask: ask, last: last,
        bid_size: bid_size, ask_size: ask_size, volume: volume,
        quote_time_ms: quote_time_ms, trade_time_ms: trade_time_ms,
        mark: mark, extra_json: extra_json
      }
    end

    def wait_for_stop
      until @stop_requested
        sleep(0.25)
      end
    end

    def log_prefix
      "[level_one:#{@job_name}]"
    end
  end
end
