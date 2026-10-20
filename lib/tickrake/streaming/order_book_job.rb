# frozen_string_literal: true

module Tickrake
  class OrderBookJob
    def initialize(runtime, scheduled_job:)
      @runtime = runtime
      @scheduled_job = scheduled_job
      @order_book_config = scheduled_job.settings
      @job_name = scheduled_job.name
      @provider = scheduled_job.provider

      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      provider_def = runtime.config.provider_definition(@provider)
      @events_writer = Tickrake::Streaming::EventStreamWriter.new(
        config: runtime.config,
        logger: runtime.logger,
        dataset_type: "order_book",
        provider: @provider,
        provider_definition: provider_def
      )
    end

    def run_session(window_start:)
      @stop_requested = false
      @subscribed_symbols = []
      @last_resolved_at = nil

      client = Tickrake::ClientFactory.new(@runtime.config).build
      stream = SchwabRb::Stream::Client.new(client)

      if @order_book_config.options_book?
        resolver = Tickrake::OrderBook::ContractResolver.new(client, @order_book_config.contracts)
        initial_symbols = resolver.resolve
        @subscribed_symbols = initial_symbols.dup
        @last_resolved_at = Time.now
        @runtime.logger.info("#{log_prefix} Resolved #{initial_symbols.size} initial contracts: #{initial_symbols.first(5).join(", ")}...")
      else
        @subscribed_symbols = @scheduled_job.universe.dup
      end

      @order_book_config.services.each do |service|
        stream.on(service.downcase.to_sym, symbols: @subscribed_symbols, fields: :all) do |event|
          handle_event(event, service: service)
        end
      end

      stream.start_async

      resolve_loop(stream: stream, resolver: @order_book_config.options_book? ? resolver : nil)
    ensure
      stream&.stop rescue nil
      @events_writer.close
    end

    def stop
      @stop_requested = true
    end

    def close; end

    private

    def resolve_loop(stream:, resolver:)
      stats_log_interval = 30
      last_stats_log = Time.now

      until @stop_requested
        sleep(0.25)

        if (Time.now - last_stats_log) >= stats_log_interval
          @events_writer&.flush_progress
          last_stats_log = Time.now
        end

        next unless resolver

        elapsed = Time.now - @last_resolved_at
        next unless elapsed >= @order_book_config.re_resolve_interval_seconds

        new_symbols = resolver.resolve
        added = new_symbols - @subscribed_symbols
        unless added.empty?
          @runtime.logger.info("#{log_prefix} Adding #{added.size} new contracts from re-resolution.")
          @order_book_config.services.each do |service|
            stream.on(service.downcase.to_sym, symbols: added, fields: :all) do |event|
              handle_event(event, service: service)
            end
          end
          @subscribed_symbols |= added
        end
        @last_resolved_at = Time.now
      end
    end

    # Order book events carry two timestamps:
    # - received_at: local collector receipt time (UTC ISO 8601). Safest cutoff for
    #   "what data did I have by time T?".
    # - book_time: market snapshot timestamp (UTC ISO 8601) from the broker/exchange feed.
    def handle_event(event, service:)
      received_at = Time.now.utc.iso8601
      entries = Array(event["content"] || event[:content] || [event])

      entries.each do |entry|
        symbol = entry["key"] || entry[:key]
        next unless symbol

        book_time_raw = entry["1"] || entry[:book_time]
        book_time = book_time_raw ? Time.at(book_time_raw.to_i / 1000.0).utc.iso8601 : nil

        @events_writer.write(
          "job_type"    => "order_book",
          "provider"    => @provider,
          "symbol"      => symbol.to_s,
          "service"     => service,
          "received_at" => received_at,
          "book_time"   => book_time,
          "bids_json"   => extract_json(entry["2"] || entry[:bids]),
          "asks_json"   => extract_json(entry["3"] || entry[:asks])
        )
      end
    end

    def extract_json(data)
      JSON.dump(data) if data
    end

    def log_prefix
      "[order_book:#{@job_name}]"
    end
  end
end
